import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.mongodb.MongoClientSettings
import com.mongodb.MongoCredential
import com.mongodb.ServerAddress
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.*
import com.mongodb.connection.ClusterSettings
import org.bson.Document
import java.io.File
import java.text.SimpleDateFormat
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import kotlin.random.Random
import kotlin.reflect.KFunction
import kotlin.reflect.full.declaredFunctions

class BenchThread(
    private val threadName: String,
    private val mongodbIps: List<String>,
    private val queryQueue: ConcurrentLinkedQueue<QueryTask>,
    private val log: MutableList<QueryExecutionLog>,
    private val startLatch: CountDownLatch,
    private val seed: Long
) : Thread(threadName) {

    private var mongoDatabase: MongoDatabase

    private val random = Random(seed)
    private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    private val municipalitiesPath = "../../data/nrwData/output/municipalities.csv"
    private val countiesPath = "../../data/nrwData/output/counties.csv"
    private val districtsPath = "../../data/nrwData/output/districts.csv"
    private val citiesPath = "../../data/nrwData/output/cities.csv"
    private val airportsPath = "../../data/nrwData/output/airports.csv"
    private val airplanetypesPath = "../../data/nrwData/output/airplanetypes.csv"

    private val municipalities = parseCSV(municipalitiesPath, setOf("name"))
    private val counties = parseCSV(countiesPath, setOf("name"))
    private val districts = parseCSV(districtsPath, setOf("name"))
    private val cities = parseCSV(citiesPath, setOf("area", "lat", "lon", "district", "name", "population"))
    private val airports = parseCSV(airportsPath, setOf("IATA", "ICAO", "Airport name", "Country", "City"))
    private val airplanetypes = File(airplanetypesPath).readLines()


    init {
        this.uncaughtExceptionHandler = UncaughtExceptionHandler { thread, exception ->
            println("Error in thread ${thread.name}: ${exception.message}")
            exception.printStackTrace()
        }

        val user = "felix"
        val password = "master"
        val mongodbClientPort = 27017

        var mongodbHosts = ArrayList<ServerAddress>();

        mongodbIps.forEach{ipAddress -> mongodbHosts.add(ServerAddress(ipAddress, mongodbClientPort))}

        val conn = MongoClients.create(
            MongoClientSettings.builder()
                .applyToClusterSettings { builder: ClusterSettings.Builder ->
                    builder.hosts(
                        mongodbHosts
                    )
                }
                .credential(
                    MongoCredential.createCredential(
                        user,
                        "admin",
                        password.toCharArray()
                    )
                )
                .build())

        mongoDatabase = conn.getDatabase(DATABASE)
    }

    override fun run() {

            val flightPointsCollection = mongoDatabase.getCollection("flightpoints")
            val flightTripCollection = mongoDatabase.getCollection("flighttrips")
            // Ensure all threads start at the same time
            startLatch.await()

            println("$threadName started executing at ${Instant.now()}.")
            println()
        try{
            while (true) {
                val task = queryQueue.poll() ?: break

                val mongoParameterValues = if (task.params != null) {
                    returnParamValues(task.params)
                } else {
                    null
                }

                val currentFunction = invokeFunctionByName(task.queryName)

                val startTime = Instant.now().toEpochMilli()

                val response = mongoParameterValues?.let { currentFunction?.call(*it.toTypedArray()) }

                val endTime = Instant.now().toEpochMilli()

                printMongoResponse(response)


                val params = task.params?.joinToString(";") ?: ""
                val parameterValues = mongoParameterValues?.joinToString(";") ?: ""

                synchronized(log) {
                    log.add(
                            QueryExecutionLog(
                                threadName = threadName,
                                queryName = task.queryName,
                                queryType = task.type,
                                params =  params,
                                paramValues = parameterValues,
                                round = 0,
                                executionIndex = 0,
                                startTime = startTime,
                                endTime = endTime,
                                startTimeSecond = 0L,
                                endTimeSecond = 0L,
                                latency = (endTime - startTime)/1000
                            )

                    )
                }

            }

        } catch (e: Exception) {
            // Catch any unexpected exceptions and print them to the console
            println("An error occurred in thread $threadName: ${e.message}")
            e.printStackTrace()
        } finally {

        }
    }

    private fun returnParamValues(params: List<String>): List<String> {
        var values = ArrayList<String>()
        for (param in params){
            val replacement = when (param) {
                "period_short" -> generateRandomTimeSpan(random=this.random, formatter = this.formatter, year=2023, mode=1)
                "period_medium" -> generateRandomTimeSpan(random=this.random, formatter = this.formatter, year=2023, mode=2)
                "period_long" -> generateRandomTimeSpan(random=this.random, formatter = this.formatter, year=2023, mode=3)
                "period" -> generateRandomTimeSpan(formatter = this.formatter, year=2023, random = this.random)
                "instant" -> generateRandomTimestamp(formatter = this.formatter, random = this.random)
                "city" -> getRandomPlace(this.cities, "name", this.random)
                "municipality" -> getRandomPlace(this.municipalities, "name", this.random)
                "county" -> getRandomPlace(this.counties, "name", this.random)
                "district" -> getRandomPlace(this.districts, "name", this.random)
                "airport" -> getRandomPlace(this.airports, "Airport name", this.random)
                "day" -> getRandomDay(random = this.random, year = 2023)
                "radius" -> (random.nextInt(100, 200) * 10).toString();
                "low_altitude" -> (random.nextInt(300, 600) * 10).toString();
                "type" -> "'${airplanetypes[random.nextInt(0, airplanetypes.size)]}'"
                else -> ""

            }
            if (replacement != null && !param.contains("period") ) {
                values.add(replacement.toString())
            }
        }
        return values
    }

    private fun printMongoResponse(response: Any?) {
        // Configure Jackson ObjectMapper for pretty JSON formatting
        val mapper = ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)

        var castedResponse  = response as List<Document>
        println("MongoDB Response:")
        castedResponse.forEach { document ->
            try {
                // Convert the Document to a JSON string and print it
                val jsonString = mapper.writeValueAsString(document.toMap())
                println(jsonString)
            } catch (e: Exception) {
                println("Error formatting document: ${document.toJson()}")
            }
        }
    }

    private fun generateRandomTimestamp(random: Random, formatter: DateTimeFormatter): String {
        val year = 2023
        val dayOfYear = random.nextInt(1, 366) // Days in the year 2023
        val hour = random.nextInt(0, 24)
        val minute = random.nextInt(0, 60)
        val second = random.nextInt(0, 60)

        // Use LocalDate.ofYearDay to get the date and then add the time
        val date = LocalDate.ofYearDay(year, dayOfYear)
        val timestamp = LocalDateTime.of(date, java.time.LocalTime.of(hour, minute, second))

        return "timestamptz'${timestamp.format(formatter)}'"
    }

    // Function to generate a random time span (period) within 2023
    private fun generateRandomTimeSpan(random: Random, formatter: DateTimeFormatter, year: Int, mode: Int = 0): Pair<SimpleDateFormat, SimpleDateFormat> {

        // Generate pseudo random timestamps based on the mode
        // 1: for short time range (0-2 days), 2: for medium time range (2 days - 1 month), 3: for long time range (1 - 12 Month)
        // 0: Full random (0 days to 12 months)
        val startDayOfYear = random.nextInt(1, 366)
        val startHour = random.nextInt(0, 24)
        val startMinute = random.nextInt(0, 60)
        val startSecond = random.nextInt(0, 60)

        val date1 = LocalDate.ofYearDay(year, startDayOfYear)
        val timestamp1 = LocalDateTime.of(date1, java.time.LocalTime.of(startHour, startMinute, startSecond))

        // Calculate the end timestamp based on the mode
        val endDate: LocalDateTime = when (mode) {
            1 -> {
                // Up to 2 days
                val daysToAdd = random.nextLong(0, 2 + 1)
                val tentativeEnd = timestamp1.plusDays(daysToAdd)
                if (tentativeEnd.year == year) tentativeEnd else timestamp1.withDayOfYear(365).withHour(23).withMinute(59).withSecond(59)
            }
            2 -> {
                // Between 2 days and 1 month
                val daysToAdd = random.nextLong(2, 30 + 1)
                val tentativeEnd = timestamp1.plusDays(daysToAdd)
                if (tentativeEnd.year == year) tentativeEnd else timestamp1.withDayOfYear(365).withHour(23).withMinute(59).withSecond(59)
            }
            3 -> {
                // Between 1 and 12 months
                val monthsToAdd = random.nextLong(1, 12 + 1)
                val tentativeEnd = timestamp1.plusMonths(monthsToAdd)
                if (tentativeEnd.year == year) tentativeEnd else timestamp1.withDayOfYear(365).withHour(23).withMinute(59).withSecond(59)
            }
            else -> {

                val randomDay = random.nextInt(1, 366)
                val randomHour = random.nextInt(0, 24)
                val randomMinute = random.nextInt(0, 60)
                val randomSecond = random.nextInt(0, 60)

                val date2 = LocalDate.ofYearDay(year, randomDay)
                LocalDateTime.of(date2, java.time.LocalTime.of(randomHour, randomMinute, randomSecond))
            }
        }

        //Ensure start is before end
        val (start, end) = if (timestamp1.isBefore(endDate)) {
            timestamp1 to endDate
        } else {
            endDate to timestamp1
        }

        return Pair(SimpleDateFormat(start.format(formatter)), SimpleDateFormat(end.format(formatter)))
    }


    private fun parseCSV(filePath: String, requiredColumns: Set<String>): List<Map<String, String>> {
        val rows = mutableSetOf<Map<String, String>>()
        val lines = File(filePath).readLines()

        if (lines.isNotEmpty()) {
            val header = lines.first().split(",")

            val indicesToKeep = header.withIndex()
                .filter { it.value in requiredColumns }
                .map { it.index }

            rows.addAll(
                lines.drop(1).map { line ->
                    val values = line.split(",")
                    indicesToKeep.associate { header[it] to values[it] }
                }.distinct()
            )
        }

        return rows.toList()
    }

    private fun getRandomPlace(
        parsedData: List<Map<String, String>>,
        columnName: String,
        random: Random
    ): String? {

        val columnValues = parsedData.mapNotNull { it[columnName] }

        if (columnValues.isEmpty()) return null

        return "'${columnValues[random.nextInt(columnValues.size)]}'"
    }

    private fun getRandomDay(random: Random, year: Int): String {

        val startDate = LocalDate.of(year, 1, 1)
        val endDate = LocalDate.of(year, 12, 31)

        val daysInYear = endDate.toEpochDay() - startDate.toEpochDay() + 1

        val randomDay = startDate.plusDays(random.nextLong(0, daysInYear))

        return "$randomDay"
    }

    private fun invokeFunctionByName(functionName: String): KFunction<*>? {
        val benchThreadClass = this::class

        val function = benchThreadClass.declaredFunctions.find { it.name == functionName }

        if (function != null) {
            return function
        } else {
            println("Function '$functionName' not found.")
            return benchThreadClass.declaredFunctions.find { it.name == "queryFlightsByPeriod" }
        }
    }


}


