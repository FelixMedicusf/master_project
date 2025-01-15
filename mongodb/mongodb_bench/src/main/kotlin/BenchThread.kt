import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.mongodb.ExplainVerbosity
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
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import javax.print.Doc
import kotlin.collections.ArrayList
import kotlin.random.Random
import kotlin.reflect.KFunction
import kotlin.reflect.full.functions
import kotlin.reflect.typeOf
import java.time.ZoneOffset

class BenchThread(
    private val threadName: String,
    private val mongodbIps: List<String>,
    private val queryQueue: ConcurrentLinkedQueue<QueryTask>,
    private val log: MutableList<QueryExecutionLog>,
    private val startLatch: CountDownLatch,
    private val givenSeed: Long
) : Thread(threadName) {

    private var mongoDatabase: MongoDatabase
    private val seed = givenSeed
    private val dateFormat = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    private val random = Random(seed)
    private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    private val municipalitiesPath = "src/main/resources/municipalities.csv"
    private val countiesPath = "src/main/resources/counties.csv"
    private val districtsPath = "src/main/resources/districts.csv"
    private val citiesPath = "src/main/resources/cities.csv"
    private val airportsPath = "src/main/resources/airports.csv"
    private val airplanetypesPath = "src/main/resources/airplanetypes.csv"

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

            val citiesCollection = mongoDatabase.getCollection("cities")
            val airportsCollection = mongoDatabase.getCollection("airports")
            val municipalitiesCollection = mongoDatabase.getCollection("municipalities")
            val countiesCollection = mongoDatabase.getCollection("counties")
            val districtsCollection = mongoDatabase.getCollection("districts")
            val flightPointsCollection = mongoDatabase.getCollection("flightpoints")
            val flightPointsTsCollection = mongoDatabase.getCollection("flightpoints_ts")
            val flightTripsCollection = mongoDatabase.getCollection("flighttrips")

            val staticCollections = listOf(citiesCollection, airportsCollection, municipalitiesCollection, countiesCollection, districtsCollection)
            val dynamicCollections = listOf(flightPointsCollection, flightPointsTsCollection, flightTripsCollection)

            // Ensure all threads start at the same time
            startLatch.await()

            println("$threadName started executing at ${Instant.now()} with seed ${this.seed}.")

        try{
            while (true) {
                val task = queryQueue.poll() ?: break

                val mongoParameters: MutableList<Any> = mutableListOf(staticCollections, dynamicCollections)
                val mongoValues: MutableList<Any> = mutableListOf()

                if (task.params != null){
                    mongoValues.addAll(returnParamValues(task.params))
                }

                println("${threadName} executing task: ${task.queryName} with parameter values: ${mongoValues}")

                mongoValues.forEach { value -> mongoParameters.add(value) }

                val currentFunction = invokeFunctionByName(task.queryName)
                val response = mongoParameters.let { params -> currentFunction.call(this, *params.toTypedArray()) }
                val endTime = Instant.now().toEpochMilli()

                printMongoResponse(response.fourth)


                val params = task.params?.joinToString(";") ?: ""
                val parameterValues = mongoValues.joinToString(";")

                synchronized(log) {
                    log.add(
                            QueryExecutionLog(
                                threadName = threadName,
                                queryName = task.queryName,
                                queryType = task.type,
                                params =  params,
                                paramValues = parameterValues.replace(",", ";"),
                                round = 0,
                                executionIndex = 0,
                                startTime = response.first,
                                endTime = response.second,
                                startTimeSecond = response.third,
                                endTimeSecond = endTime,
                                latency = (endTime - response.first),
                                records = response.fourth.size
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

    private fun returnParamValues(params: List<String>): List<Any> {
        var values = ArrayList<Any>()
        for (param in params){
            val replacement = when (param) {
                "period_short" -> generateRandomTimeSpan(random=this.random, formatter = this.formatter, year=2023, mode=1)
                "period_medium" -> generateRandomTimeSpan(random=this.random, formatter = this.formatter, year=2023, mode=2)
                "period_long" -> generateRandomTimeSpan(random=this.random, formatter = this.formatter, year=2023, mode=3)
                "period" -> generateRandomTimeSpan(formatter = this.formatter, year=2023, random = this.random)
                "instant" -> generateRandomTimestamp(formatter = this.formatter, random = this.random)
                "day" -> getRandomDay(random = this.random, year = 2023)
                "city" -> getRandomPlace(this.cities, "name", this.random)
                "airport" -> getRandomPlace(this.airports, "Airport name", this.random)
                "municipality" -> getRandomPlace(this.municipalities, "name", this.random)
                "county" -> getRandomPlace(this.counties, "name", this.random)
                "district" -> getRandomPlace(this.districts, "name", this.random)
                "point" -> getRandomPoint(this.random, listOf(listOf(6.212909, 52.241256), listOf(8.752841, 50.53438)))
                "radius" -> (random.nextDouble(0.25, 0.5) * 10)/6378.1
                "low_altitude" -> (random.nextInt(300, 600) * 10)
                "type" -> "${airplanetypes[random.nextInt(0, airplanetypes.size)]}"
                "distance" -> (random.nextInt(10, 100) * 10)
                else -> ""

            }
            if (replacement != null) {
                values.add(replacement)
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

        return timestamp.format(formatter)
    }

    // Function to generate a random time span (period) within 2023
    private fun generateRandomTimeSpan(random: Random, formatter: DateTimeFormatter, year: Int, mode: Int = 0): List<String> {

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
                val secondsToShift = random.nextLong(0, 172801)
                val tentativeEnd = if (random.nextBoolean()) {
                    timestamp1.plusSeconds(secondsToShift)
                } else {
                    timestamp1.minusSeconds(secondsToShift)
                }
                if (tentativeEnd.year == year){
                    tentativeEnd
                } else if (tentativeEnd.year > year) {
                    timestamp1.withDayOfYear(365).withHour(23).withMinute(59).withSecond(59)
                } else if (tentativeEnd.year < year){
                    timestamp1.withDayOfYear(1).withHour(1).withMinute(1).withSecond(1)
                } else timestamp1
            }
            2 -> {
                // Between 2 days and 30 days
                val secondsToShift = random.nextLong(172800, 2592001)
                val tentativeEnd = if (random.nextBoolean()) {
                    timestamp1.plusSeconds(secondsToShift)
                } else {
                    timestamp1.minusSeconds(secondsToShift)
                }
                if (tentativeEnd.year == year){
                    tentativeEnd
                } else if (tentativeEnd.year > year) {
                    timestamp1.withDayOfYear(365).withHour(23).withMinute(59).withSecond(59)
                } else if (tentativeEnd.year < year){
                    timestamp1.withDayOfYear(1).withHour(1).withMinute(1).withSecond(1)
                } else timestamp1
            }
            3 -> {
                // Between 1 and 12 months
                val secondsToShift = random.nextLong(259200, 31536001)
                val tentativeEnd = if (random.nextBoolean()) {
                    timestamp1.plusSeconds(secondsToShift)
                } else {
                    timestamp1.minusSeconds(secondsToShift)
                }
                if (tentativeEnd.year == year){
                    tentativeEnd
                } else if (tentativeEnd.year > year) {
                    timestamp1.withDayOfYear(365).withHour(23).withMinute(59).withSecond(59)
                } else if (tentativeEnd.year < year){
                    timestamp1.withDayOfYear(1).withHour(1).withMinute(1).withSecond(1)
                } else timestamp1
            }
            else -> {
                // Full random (0 days to 12 months)
                val randomDay = random.nextInt(1, 366)
                val randomHour = random.nextInt(0, 24)
                val randomMinute = random.nextInt(0, 60)
                val randomSecond = random.nextInt(0, 60)
                val date2 = LocalDate.ofYearDay(year, randomDay)
                LocalDateTime.of(date2, java.time.LocalTime.of(randomHour, randomMinute, randomSecond))
            }
        }

        // Ensure start is before end
        val (start, end) = if (timestamp1.isBefore(endDate)) {
            timestamp1 to endDate
        } else {
            endDate to timestamp1
        }

        return listOf(start.format(formatter), end.format(formatter))
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

        return columnValues[random.nextInt(columnValues.size)]
    }

    private fun getRandomPoint(random: Random, rectangle: List<List<Double>>): List<Double> {
        val upperLeftLon = rectangle[0][0]
        val upperLeftLat = rectangle[0][1]
        val bottomRightLon = rectangle[1][0]
        val bottomRightLat = rectangle[1][1]

        val randomLon = upperLeftLon + random.nextDouble() * (bottomRightLon - upperLeftLon)

        val randomLat = bottomRightLat + random.nextDouble() * (upperLeftLat - bottomRightLat)

        // Return the random point
        return listOf(randomLon, randomLat)
    }


    private fun getRandomDay(random: Random, year: Int): String {

        val startDate = LocalDate.of(year, 1, 1)
        val endDate = LocalDate.of(year, 12, 31)

        val daysInYear = endDate.toEpochDay() - startDate.toEpochDay() + 1

        val randomDay = startDate.plusDays(random.nextLong(0, daysInYear))

        return "$randomDay"
    }


    private fun invokeFunctionByName(functionName: String): KFunction<Quadrupel<Long, Long, Long, List<Document>>> {
        val benchThreadClass = this::class

        // Search in all functions accessible in the declaring file
        val function = benchThreadClass.functions.find { it.name == functionName }
            ?: throw IllegalArgumentException("Function $functionName not found in ${benchThreadClass.simpleName}")

        // Validate the return type
        if (function.returnType != typeOf<Quadrupel<Long, Long, Long, List<Document>>>()) {
            throw IllegalArgumentException("Function $functionName does not return the expected type")
        }

        @Suppress("UNCHECKED_CAST") // Safe because of the type check above
        return function as KFunction<Quadrupel<Long, Long, Long, List<Document>>>
    }
















    fun countActiveFlightsInPeriod(
        staticCollections: List<MongoCollection<Document>>,
        dynamicCollections: List<MongoCollection<Document>>,
        period: List<String>,
    ): Quadrupel<Long, Long, Long, List<Document>> {

        val flightPointsTsCollection = dynamicCollections[1]

        val startDate: Date = dateFormat.parse(period[0])
        val endDate: Date = dateFormat.parse(period[1])

        val pipeline =  listOf(
            Document("\$match", Document("timestamp", Document("\$gte", startDate).append("\$lte", endDate))),
            Document(
                "\$group", Document()
                    .append(
                        "_id", Document()
                            .append("flightId", "\$metadata.flightId")
                            .append("track", "\$metadata.track")
                    )
            ),
            // Stage 3: Optional - Flatten the _id for cleaner output
            Document("\$count", "count"),
            Document("\$project", Document("period", Document("\$concat", listOf(period[0], " - ", period[1]))).append("count", 1)
        )
        )

        val queryTimeStart = Instant.now().toEpochMilli()
        // Execute the aggregation
        val results = flightPointsTsCollection.aggregate(pipeline)

        // Convert results to a list of Documents
        return Quadrupel(queryTimeStart, 0, 0, results.into(mutableListOf()))

    }

    fun locationOfAirplaneAtInstant(
        staticCollections: List<MongoCollection<Document>>,
        dynamicCollections: List<MongoCollection<Document>>,
        instant: String
    ): Quadrupel<Long, Long, Long, List<Document>> {

        val flightPointsCollection = dynamicCollections[1]

        val timestamp: Date = dateFormat.parse(instant)
        val pipeline = listOf(
            Document(
                "\$match", Document("timestamp", timestamp) // Filter documents with exact timestamp
            ),
            Document(
                "\$project", Document(
                    "metadata.flightId", 1) // Include flight ID
                    .append("time", instant) // Add the provided instant as a field
                    .append(
                        "altitude", "\$altitude"

                    )
                    .append(
                        "longitude",
                        Document("\$arrayElemAt", listOf("\$location.coordinates", 0))
                    )
                    .append(
                        "latitude",
                        Document("\$arrayElemAt", listOf("\$location.coordinates", 0))
                    )
            ),
            Document(
                "\$project", Document(
                   "flightId" , "\$metadata.flightId"
                ).append("_id",0)
                    .append("time", 1)
                    .append("altitude", "\$altitude")
                    .append("location", Document("\$concat", listOf(
                        "POINT(",
                        Document("\$toString", "\$longitude"), " ",
                        Document("\$toString", "\$latitude"), ")"
                    )))
            )
        )

        val queryTimeStart = Instant.now().toEpochMilli()
        // Execute the aggregation
        val results = flightPointsCollection.aggregate(pipeline)

        // Convert results to a list of Documents
        return Quadrupel(queryTimeStart, 0, 0, results.into(mutableListOf()))
    }

    // not being used for the benchmark (similar spatiotemporal query)
    fun flightTimeLowAltitude(
        staticCollections: List<MongoCollection<Document>>,
        dynamicCollections: List<MongoCollection<Document>>,
        period: List<String>,
        low_altitude: Int
    ): Quadrupel<Long, Long, Long, List<Document>> {

        val flightPointsTsCollection = dynamicCollections[1]

        val startDate: Date = dateFormat.parse(period[0])
        val endDate: Date = dateFormat.parse(period[1])

        val pipeline = listOf(

            Document(
                "\$match", Document(
                    "\$and", listOf(
                        Document(
                            "timestamp", Document("\$gte", startDate).append("\$lte", endDate)
                        ),
                        Document(
                            "altitude", Document("\$lt", low_altitude)
                        )
                    )
                )
            ),

            Document(
                "\$project", Document(
                    "metadata.flightId", 1
                ).append(
                    "durations", Document(
                        "\$map", Document(
                            "input", Document(
                                "\$range", listOf(0, Document("\$subtract", listOf(Document("\$size", "\$points"), 1)))
                            )
                        ).append(
                            "as", "index"
                        ).append(
                            "in", Document(
                                "altitude", Document("\$arrayElemAt", listOf("\$points.altitude", "\$\$index"))
                            ).append(
                                "startTime", Document("\$arrayElemAt", listOf("\$points.timestamp", "\$\$index"))
                            ).append(
                                "endTime", Document("\$arrayElemAt", listOf("\$points.timestamp", Document("\$add", listOf("\$\$index", 1))))
                            )
                        )
                    )
                )
            ),

            Document(
                "\$project", Document(
                    "flightId", 1
                ).append(
                    "totalTime", Document(
                        "\$sum", Document(
                            "\$map", Document(
                                "input", "\$durations"
                            ).append(
                                "as", "duration"
                            ).append(
                                "in", Document(
                                    "\$divide", listOf(
                                        Document("\$subtract", listOf("\$\$duration.endTime", "\$\$duration.startTime")),
                                        1000
                                    )
                                )
                            )
                        )
                    )
                ).append(
                    "timeBelow4000", Document(
                        "\$sum", Document(
                            "\$map", Document(
                                "input", "\$durations"
                            ).append(
                                "as", "duration"
                            ).append(
                                "in", Document(
                                    "\$cond", listOf(
                                        Document("\$lt", listOf("\$\$duration.altitude", 4000)),
                                        Document(
                                            "\$divide", listOf(
                                                Document("\$subtract", listOf("\$\$duration.endTime", "\$\$duration.startTime")),
                                                1000
                                            )
                                        ),
                                        0
                                    )
                                )
                            )
                        )
                    )
                )
            ),

            Document(
                "\$addFields", Document(
                    "ratio", Document(
                        "\$cond", listOf(
                            Document("\$gt", listOf("\$totalTime", 0)),
                            Document("\$divide", listOf("\$timeBelow4000", "\$totalTime")),
                            0
                        )
                    )
                )
            ),

            Document(
                "\$project", Document(
                    "_id", 0
                ).append(
                    "flightId", 1
                ).append(
                    "timeBelow4000", 1
                ).append(
                    "totalTime", 1
                ).append(
                    "ratio", 1
                )
            )
        )

        val queryTimeStart = Instant.now().toEpochMilli()

        return Quadrupel(queryTimeStart, 0, 0, flightPointsTsCollection.aggregate(pipeline).allowDiskUse(true).toList())
    }

    fun averageHourlyFlightsDuringDay(
        staticCollections: List<MongoCollection<Document>>,
        dynamicCollections: List<MongoCollection<Document>>,
        day: String
    ): Quadrupel<Long, Long, Long, List<Document>> {

        val flightPointsTsCollection = dynamicCollections[1]

        val date = LocalDate.parse(day.trim('\'')) // Trim single quotes and parse

        val startOfDay = Date.from(date.atStartOfDay().toInstant(ZoneOffset.UTC))
        val endOfDay = Date.from(date.plusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC))

        // List of all hours (0-23)
        val hours = (0..23).toList()

        val pipeline = listOf(
            // Step 1: Filter documents for the specified day
            Document("\$match", Document("timestamp", Document("\$gte", startOfDay).append("\$lt", endOfDay))),

            // Step 2: Add a field for the hour of the timestamp
            Document("\$addFields", Document("hour", Document("\$hour", "\$timestamp"))),

            // Step 3: Group by hour and collect distinct flightIds
            Document("\$group", Document().apply {
                append("_id", "\$hour") // Group by hour
                append("distinctFlights", Document("\$addToSet", "\$metadata.flightId")) // Collect distinct flightIds
            }),

            // Step 4: Calculate the number of flights per hour
            Document("\$project", Document().apply {
                append("_id", 0) // Exclude MongoDB's default _id field
                append("hour", "\$_id") // Rename the _id field to hour
                append("flightCount", Document("\$size", "\$distinctFlights")) // Count the distinct flights
            }),

            // Step 5: Sort by hour
            Document("\$sort", Document("hour", 1))
        )


        val queryTimeStart = Instant.now().toEpochMilli()

        return Quadrupel(queryTimeStart, 0, 0, flightPointsTsCollection.aggregate(pipeline).toList())
    }

    fun flightsWithLocalOriginDestinationInPeriod(
        staticCollections: List<MongoCollection<Document>>,
        dynamicCollections: List<MongoCollection<Document>>,
        period: List<String>
    ): Quadrupel<Long, Long, Long, List<Document>>{

        val airportsCollection = staticCollections[1]

        val startDate: Date = dateFormat.parse(period[0])
        val endDate: Date = dateFormat.parse(period[1])

        val pipeline = listOf(
            // Stage 1: Lookup cities for airports
            Document(
                "\$lookup", Document("from", "cities")
                    .append("localField", "City")
                    .append("foreignField", "name")
                    .append("as", "cityDetails")
            ),
            Document("\$match", Document("cityDetails", Document("\$ne", emptyList<Any>()))),
            Document("\$project", Document("ICAO", 1)
                .append("cityName", Document("\$arrayElemAt", listOf("\$cityDetails.name", 0)))
                .append("_id", 0)),

            // Stage 2: Group local airports into a list
            Document("\$group", Document("_id", null)
                .append("localAirports", Document("\$addToSet", "\$ICAO"))),

            // Stage 3: Pass localAirports list to the lookup
            Document("\$lookup", Document("from", "flightpoints_ts")
                .append("let", Document("localAirports", "\$localAirports"))
                .append("pipeline", listOf(

                    Document("\$match", Document("timestamp", Document("\$gte", startDate).append("\$lte", endDate))),
                    Document(
                        "\$group", Document()
                            .append(
                                "_id", Document()
                                    .append("flightId", "\$metadata.flightId")
                            ).append("originAirport", Document("\$first", "\$metadata.originAirport")) // Include 'originAirport'
                            .append("destinationAirport", Document("\$first", "\$metadata.destinationAirport")) // Include 'destinationAirport'
                            .append("airplaneType", Document("\$first", "\$metadata.airplaneType"))
                    ),
                    // Match flighttrips where origin or destination matches local airports
                    Document(
                        "\$match", Document("\$expr", Document("\$or", listOf(
                            Document("\$in", listOf("\$originAirport", "\$\$localAirports")),
                            Document("\$in", listOf("\$destinationAirport", "\$\$localAirports"))
                        )))
                    )
                ))
                .append("as", "filteredFlightTrips")
            ),
            Document("\$unwind", "\$filteredFlightTrips"),
            Document(
                "\$lookup", Document("from", "airports")
                    .append("localField", "filteredFlightTrips.originAirport")
                    .append("foreignField", "ICAO")
                    .append("as", "cityNameOrigin")
            ),
            Document(
                "\$lookup", Document("from", "airports")
                    .append("localField", "filteredFlightTrips.destinationAirport")
                    .append("foreignField", "ICAO")
                    .append("as", "cityNameDestination")
            ),
            // Stage 4: Project final fields
            Document("\$project", Document()
                .append("_id", 0)
                .append("flightId", "\$filteredFlightTrips.flightId")
                .append("originAirport", "\$filteredFlightTrips.originAirport")
                .append("destinationAirport", "\$filteredFlightTrips.destinationAirport")
                .append("originCity", "\$cityNameOrigin.City")
                .append("destinationCity", "\$cityNameDestination.City")
                .append("airplaneType", "\$filteredFlightTrips.airplaneType")
            )
        )

        val queryTimeStart = Instant.now().toEpochMilli()

        return Quadrupel(queryTimeStart, 0, 0, airportsCollection.aggregate(pipeline).toList())
    }

    fun airportUtilizationInPeriod(
        staticCollections: List<MongoCollection<Document>>,
        dynamicCollections: List<MongoCollection<Document>>,
        period:List<String>
    ): Quadrupel<Long, Long, Long, List<Document>> {

        val flightPointsTsCollection = dynamicCollections[1]

        val startDate: Date = dateFormat.parse(period[0])
        val endDate: Date = dateFormat.parse(period[1])

        val pipeline = listOf(
            // Stage 1: Filter flights based on timeRange overlap with the period
            Document("\$match", Document("timestamp", Document("\$gte", startDate).append("\$lte", endDate))),
            Document(
                "\$group", Document()
                    .append(
                        "_id", Document()
                            .append("flightId", "\$metadata.flightId")
                    ).append("originAirport", Document("\$first", "\$metadata.originAirport")) // Include 'originAirport'
                    .append("destinationAirport", Document("\$first", "\$metadata.destinationAirport")) // Include 'destinationAirport'
                    .append("airplaneType", Document("\$first", "\$metadata.airplaneType"))
            ),
            Document(
                "\$facet", Document("departures", listOf(
                    Document(
                        "\$group", Document("_id", "\$originAirport")
                            .append("departure_count", Document("\$sum", 1))
                    )
                ))
                    .append("arrivals", listOf(
                        Document(
                            "\$group", Document("_id", "\$destinationAirport")
                                .append("arrival_count", Document("\$sum", 1))
                        )
                    ))
            ),

            // Combine results
            Document(
                "\$project", Document("combined", Document("\$concatArrays", listOf("\$departures", "\$arrivals")))
            ),
            Document(
                "\$unwind", "\$combined"
            ),
            Document(
                "\$replaceRoot", Document("newRoot", "\$combined")
            ),
            Document(
                "\$group", Document("_id", "\$_id")
                    .append("departure_count", Document("\$sum", Document("\$ifNull", listOf("\$departure_count", 0))))
                    .append("arrival_count", Document("\$sum", Document("\$ifNull", listOf("\$arrival_count", 0))))
            ),
            // Add traffic_count (sum of arrivals and departures)
            Document(
                "\$addFields", Document("traffic_count", Document("\$add", listOf("\$departure_count", "\$arrival_count")))
            ),
            // Sort by traffic_count, departure_count, and arrival_count
            Document(
                "\$sort", Document("traffic_count", -1)
                    .append("departure_count", -1)
                    .append("arrival_count", -1)
            )
        )



        val queryTimeStart = Instant.now().toEpochMilli()
        val result = flightPointsTsCollection.aggregate(pipeline).toList()

        return Quadrupel(queryTimeStart, 0, 0, result)
    }



    // spatial queries
    fun flightsInCityRadius(
        staticCollections: List<MongoCollection<Document>>,
        dynamicCollections: List<MongoCollection<Document>>,
        cityName: String,
        radius: Double
    ):Quadrupel<Long, Long, Long, List<Document>> {

        val citiesCollection = staticCollections[0]
        val flightPointsTsCollection = dynamicCollections[1]

        val firstPipeline = listOf(
            Document("\$match", Document("name", cityName)),
            Document("\$project", Document("location.coordinates", 1).append("_id", 0).append("name", 1))
        )


        val queryTimeStartFirstQuery = Instant.now().toEpochMilli()

        val firstResponse = citiesCollection.aggregate(firstPipeline).toList()
        val queryEndTimeFirstQuery = Instant.now().toEpochMilli()

        val name = firstResponse.get(0).getString("name")
        val coordinates = firstResponse.flatMap { document ->
            val location = document.get("location", Document::class.java) // Get 'location' sub-document
            val coords = location?.get("coordinates", List::class.java) // Get 'coordinates' as a List
            coords?.map { it as Double } ?: emptyList() // Ensure null-safe mapping
        }

        val secondPipeline = listOf(
            Document("\$match", Document("location",
                Document("\$geoWithin",
                    Document("\$centerSphere", listOf(coordinates, radius))
                )
            )),

            Document(
                "\$group", Document()
                    .append(
                        "_id", Document()
                            .append("flightId", "\$metadata.flightId")
                            .append("track", "\$metadata.track")
                    ).append("originAirport", Document("\$first", "\$metadata.originAirport")) // Include 'originAirport'
                    .append("destinationAirport", Document("\$first", "\$metadata.destinationAirport")) // Include 'destinationAirport'
                    .append("airplaneType", Document("\$first", "\$metadata.airplaneType"))
            )
        )

        val queryTimeStartSecondQuery = Instant.now().toEpochMilli()
        val secondResponse = flightPointsTsCollection.aggregate(secondPipeline).toList()


        return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartSecondQuery, secondResponse)

    }

    fun flightsIntersectingMunicipalities(
        staticCollections: List<MongoCollection<Document>>,
        dynamicCollections: List<MongoCollection<Document>>,
        municipalityName: String
    ):Quadrupel<Long, Long, Long, List<Document>> {

        val municipalitiesCollection = staticCollections[2]
        val flightTripsCollection = dynamicCollections[2]

        val firstPipeline = listOf(
            Document("\$match", Document("name", municipalityName)),
            Document("\$project", Document("polygon.coordinates", 1).append("_id", 0).append("name", 1))
        )

        val queryTimeStartFirstQuery = Instant.now().toEpochMilli()

        val firstResponse = municipalitiesCollection.aggregate(firstPipeline).toList()

        val queryEndTimeFirstQuery = Instant.now().toEpochMilli()
        //println(firstResponse)
        val name = firstResponse.get(0).getString("name")
        val polygonCoordinates = firstResponse.mapNotNull { document ->
            val polygon = document.get("polygon", Document::class.java) // Get 'polygon' sub-document
            polygon?.get("coordinates", List::class.java) // Get 'coordinates' as a List
        }[0]

        val secondPipeline = listOf(
            Document("\$match", Document("trajectory",
                Document("\$geoIntersects",
                    Document("\$geometry", Document("type", "Polygon").append("coordinates", polygonCoordinates))
                )
            )),
            Document("\$project", Document("flightId", 1).append("airplaneType", 1).append("_id", 0).append("originAirport", 1).append("destinationAirport", 1))
        )


        val queryTimeStartSecondQuery = Instant.now().toEpochMilli()
        val secondResponse = flightTripsCollection.aggregate(secondPipeline).toList()


        return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartSecondQuery, secondResponse)

    }

    fun countFlightsInCounties(
        staticCollections: List<MongoCollection<Document>>,
        dynamicCollections: List<MongoCollection<Document>>,
        countyName: String
    ): Quadrupel<Long, Long, Long, List<Document>> {

        val countiesCollection = staticCollections[3]
        val flightTripsCollection = dynamicCollections[2]

        val firstPipeline = listOf(
            Document("\$match", Document("name", countyName)),
            Document("\$project", Document("polygon.coordinates", 1).append("_id", 0).append("name", 1))
        )

        val queryTimeStartFirstQuery = Instant.now().toEpochMilli()

        val firstResponse = countiesCollection.aggregate(firstPipeline).toList()

        val queryEndTimeFirstQuery = Instant.now().toEpochMilli()
        //println(firstResponse)
        val name = firstResponse.get(0).getString("name")
        val polygonCoordinates = firstResponse.mapNotNull { document ->
            val polygon = document.get("polygon", Document::class.java) // Get 'polygon' sub-document
            polygon?.get("coordinates", List::class.java) // Get 'coordinates' as a List
        }[0]

        val secondPipeline = listOf(
            Document(
                "\$match", Document(
                    "trajectory",
                    Document(
                        "\$geoIntersects",
                        Document("\$geometry", Document("type", "Polygon").append("coordinates", polygonCoordinates))
                    )
                )
            ),
            Document("\$project",
                Document("flightId", 1).append("airplaneType", 1).append("_id", 0).append("originAirport", 1)
                    .append("destinationAirport", 1)
            ),
            Document("\$count", "flight_count")
        )


        val queryTimeStartSecondQuery = Instant.now().toEpochMilli()
        val secondResponse = flightTripsCollection.aggregate(secondPipeline).toList()


        return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartSecondQuery, secondResponse)
    }

    fun flightsCloseToMainCitiesLowAltitude(
        staticCollections: List<MongoCollection<Document>>,
        dynamicCollections: List<MongoCollection<Document>>,
        low_altitude: Int,
        radius: Double
    ): Quadrupel<Long, Long, Long, List<Document>> {

        val citiesCollection = staticCollections[0]
        val flightPointsCollection = dynamicCollections[0]

        val firstPipeline = listOf(
            Document("\$match", Document("population", Document("\$gt", 200000))),
            Document("\$project", Document("coordinates", "\$location.coordinates").append("_id", 0).append("name", 1)),
            Document("\$group", Document("_id", null)
                .append("coordinatePairs", Document("\$push", "\$coordinates"))
                .append("names", Document("\$push", "\$name"))
            )
        )

        val queryTimeStartFirstQuery = Instant.now().toEpochMilli()

        val firstResponse = citiesCollection.aggregate(firstPipeline).toList()
        val queryEndTimeFirstQuery = Instant.now().toEpochMilli()

        if (firstResponse.isEmpty()) {
            println("No cities found matching the criteria.")
            return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartFirstQuery, emptyList())
        }

        val coordinatePairs = firstResponse.firstOrNull()?.get("coordinatePairs", List::class.java)
        val geoWithinConditions = coordinatePairs?.map { pair ->
            Document("location", Document("\$geoWithin", Document("\$centerSphere", listOf(pair, radius))))
        }

        if (geoWithinConditions.isNullOrEmpty()) {
            println("No coordinate pairs to create geoWithin conditions.")
            return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartFirstQuery, emptyList())
        }

        val secondPipeline = listOf(
            Document("\$match", Document("altitude", Document("\$lt", low_altitude))),
            Document("\$match", Document("\$or", geoWithinConditions)),
            Document("\$project", Document("flightId", 1).append("altitude", 1).append("airplaneType", 1).append("track", 1).append("_id", 0).append("timestamp", 1)),
            Document("\$group", Document("_id", Document("flightId", "\$flightId")
                .append("altitude", "\$altitude")
                .append("airplaneType", "\$airplaneType")
                .append("timestamp", "\$timestamp")))
        )

        val queryTimeStartSecondQuery = Instant.now().toEpochMilli()
        val secondResponse = flightPointsCollection.aggregate(secondPipeline).toList()

        return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartSecondQuery, secondResponse)
    }

    fun flightsOnlyInOneDistrict(
        staticCollections: List<MongoCollection<Document>>,
        dynamicCollections: List<MongoCollection<Document>>,
        districtName: String
    ):Quadrupel<Long, Long, Long, List<Document>>{

        val districtsCollection = staticCollections[4]
        val flightTripsCollection = dynamicCollections[2]

        val firstPipeline = listOf(
            Document("\$match", Document("name", districtName)),
            Document("\$project", Document("polygon.coordinates", 1).append("_id", 0).append("name", 1))
        )

        val queryTimeStartFirstQuery = Instant.now().toEpochMilli()

        val firstResponse = districtsCollection.aggregate(firstPipeline).toList()

        val queryEndTimeFirstQuery = Instant.now().toEpochMilli()
        //println(firstResponse)
        val name = firstResponse.get(0).getString("name")
        val polygonCoordinates = firstResponse.mapNotNull { document ->
            val polygon = document.get("polygon", Document::class.java) // Get 'polygon' sub-document
            polygon?.get("coordinates", List::class.java) // Get 'coordinates' as a List
        }[0]

        val secondPipeline = listOf(
            Document(
                "\$match", Document(
                    "trajectory",
                    Document(
                        "\$geoWithin",
                        Document("\$geometry", Document("type", "Polygon").append("coordinates", polygonCoordinates))
                    )
                )
            ),
            Document("\$project",
                Document("flightId", 1).append("airplaneType", 1).append("track", 1).append("_id", 0).append("originAirport", 1)
                    .append("destinationAirport", 1)
            )
        )

        val queryTimeStartSecondQuery = Instant.now().toEpochMilli()
        val secondResponse = flightTripsCollection.aggregate(secondPipeline).toList()
        println(secondResponse.size)

        return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartSecondQuery, secondResponse)


    }

    fun countiesLandingsDepartures(
        staticCollections: List<MongoCollection<Document>>,
        dynamicCollections: List<MongoCollection<Document>>,
        countyName: String): Quadrupel<Long, Long, Long, List<Document>> {

        val countiesCollection = staticCollections[3]
        val citiesCollection = staticCollections[0]

        val firstPipeline = listOf(
            Document("\$match", Document("name", countyName)),
            Document("\$project", Document("polygon.coordinates", 1).append("_id", 0).append("name", 1))
        )

        val queryTimeStartFirstQuery = Instant.now().toEpochMilli()
        val firstResponse = countiesCollection.aggregate(firstPipeline).toList()
        val queryEndTimeFirstQuery = Instant.now().toEpochMilli()

        val name = firstResponse.get(0).getString("name")
        val polygonCoordinates = firstResponse.mapNotNull { document ->
            val polygon = document.get("polygon", Document::class.java)
            polygon?.get("coordinates", List::class.java)
        }[0]

        //println(polygonCoordinates)

        val secondPipeline = listOf(

            Document("\$match",
                Document("location",
                    Document("\$geoWithin",
                        Document("\$geometry",
                            Document("type", "Polygon").append("coordinates", polygonCoordinates)
                        )
                    )
                )
            ),
            Document("\$project", Document("name", 1)),

            Document("\$lookup", Document("from", "airports")
                .append("localField", "name")
                .append("foreignField", "City")
                .append("as", "airports_in_county")
            ),

            Document("\$unwind", "\$airports_in_county"),

            Document("\$group", Document("_id", null)
                .append("icaoCodes", Document("\$addToSet", "\$airports_in_county.ICAO"))
            ),

            Document("\$lookup", Document("from", "flighttrips")
                .append("let", Document("icaoCodes", "\$icaoCodes"))
                .append("pipeline", listOf(
                    Document("\$match", Document("\$expr", Document(
                        "\$or", listOf(
                            Document("\$in", listOf("\$originAirport", "\$\$icaoCodes")),
                            Document("\$in", listOf("\$destinationAirport", "\$\$icaoCodes"))
                        )
                    ))),
                    Document("\$project", Document("originAirport", 1).append("destinationAirport", 1))
                ))
                .append("as", "related_flights")
            ),

            // Step 8: Count arrivals and departures
            Document("\$unwind", "\$related_flights"),
            Document("\$group", Document("_id", null)
                .append("arrivals", Document("\$sum", Document("\$cond", listOf(
                    Document("\$in", listOf("\$related_flights.destinationAirport", "\$icaoCodes")), 1, 0
                ))))
                .append("departures", Document("\$sum", Document("\$cond", listOf(
                    Document("\$in", listOf("\$related_flights.originAirport", "\$icaoCodes")), 1, 0
                ))))
            ),
            // Step 9: Calculate overall traffic
            Document("\$addFields", Document("overall_traffic", Document("\$add", listOf("\$arrivals", "\$departures")))),
            Document("\$project", Document("_id", 0))
        )
        val queryTimeStartSecondQuery = Instant.now().toEpochMilli()
        val response = citiesCollection.aggregate(secondPipeline).toList()

        return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartSecondQuery, response)

    }

    fun flightClosestToPoint(
        staticCollections: List<MongoCollection<Document>>,
        dynamicCollections: List<MongoCollection<Document>>,
        coordinates: List<Double>,
        maxDistance: Int,
    ): Quadrupel<Long, Long, Long, List<Document>>{

        val flightTripsCollection = dynamicCollections[2]

        val pipeline = listOf(
            Document("\$geoNear", Document()
                .append("near", Document("type", "Point").append("coordinates", coordinates))
                .append("distanceField", "dist.calculated")
                .append("maxDistance", maxDistance)
                .append("key", "trajectory")
                .append("includeLocs", "dist.location")
                .append("spherical", true)
            ),
            Document("\$project", Document()
                .append("airplaneType", 1)
                .append("originAirport", 1)
                .append("destinationAirport", 1)
                .append("calculatedDistance", "\$dist.calculated")
                .append("flightId", 1)
                .append("_id", 0)
            )
        )

        val queryStartTime = Instant.now().toEpochMilli()
        val response = flightTripsCollection.aggregate(pipeline).toList()

        return Quadrupel(queryStartTime, 0, 0, response)
    }



    // spatiotemporal queries
    fun flightsInCountyInPeriod(
        staticCollections: List<MongoCollection<Document>>,
        dynamicCollections: List<MongoCollection<Document>>,
        period: List<String>,
        countyName: String
    ): Quadrupel<Long, Long, Long, List<Document>>{

        val countiesCollection = staticCollections[3]
        val flightPointsTsCollection = dynamicCollections[1]

        val startDate: Date = dateFormat.parse(period[0])
        val endDate: Date = dateFormat.parse(period[1])

        val firstPipeline = listOf(
            Document("\$match", Document("name", countyName)),
            Document("\$project", Document("polygon.coordinates", 1).append("_id", 0).append("name", 1))
        )

        val queryTimeStartFirstQuery = Instant.now().toEpochMilli()
        val firstResponse = countiesCollection.aggregate(firstPipeline).toList()
        val queryEndTimeFirstQuery = Instant.now().toEpochMilli()

        val name = firstResponse.get(0).getString("name")
        val polygonCoordinates = firstResponse.mapNotNull { document ->
            val polygon = document.get("polygon", Document::class.java)
            polygon?.get("coordinates", List::class.java)
        }[0]

        var secondPipeline = listOf(
            Document(
                "\$match", Document(
                    "\$and", listOf(
                        Document(
                            "location", Document(
                                "\$geoWithin", Document(
                                    "\$geometry", Document("type", "Polygon").append("coordinates", polygonCoordinates)
                                )
                            )
                        ),
                        Document(
                            "timestamp", Document(
                                "\$gte", startDate
                            ).append(
                                "\$lte", endDate
                            )
                        )
                    )
                )
            ),
            Document("\$group", Document("_id", "\$flightId"))
        )

        secondPipeline = listOf(
            Document(
                "\$match", Document(
                    "timestamp", Document()
                        .append("\$gte", startDate) // Start of the period
                        .append("\$lte", endDate) // End of the period
                )
            ),
            Document(
                "\$match", Document(
                    "location", Document(
                        "\$geoWithin", Document(
                            "\$geometry", Document("type", "Polygon").append("coordinates", polygonCoordinates)
                        )
                    )
                )
            ),
            Document(
                "\$group", Document(
                    "_id", Document(
                        "flightId", "\$metadata.flightId"
                    ).append(
                        "track", "\$metadata.track"
                    )
                )
            ),
            Document(
                "\$project", Document(
                    "flightId", "\$_id.flightId"
                ).append(
                    "track", "\$_id.track"
                ).append(
                    "_id", 0
                )
            )
        )

        val queryTimeStartSecondQuery = Instant.now().toEpochMilli()
        val response = flightPointsTsCollection.aggregate(secondPipeline).toList()

        return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartSecondQuery, response)

    }

    fun pairOfFlightsInMunicipalityInPeriod(
        staticCollections: List<MongoCollection<Document>>,
        dynamicCollections: List<MongoCollection<Document>>,
        period: List<String>,
        municipalityName: String
    ): Quadrupel<Long, Long, Long, List<Document>>{

        val municipalitiesCollection = staticCollections[2]
        val flightPointsTsCollection = dynamicCollections[1]

        val startDate: Date = dateFormat.parse(period[0])
        val endDate: Date = dateFormat.parse(period[1])

        val firstPipeline = listOf(
            Document("\$match", Document("name", municipalityName)),
            Document("\$project", Document("polygon.coordinates", 1).append("_id", 0).append("name", 1))
        )

        val queryTimeStartFirstQuery = Instant.now().toEpochMilli()

        val firstResponse = municipalitiesCollection.aggregate(firstPipeline).toList()

        val queryEndTimeFirstQuery = Instant.now().toEpochMilli()
        //println(firstResponse)
        val name = firstResponse.get(0).getString("name")
        var polygonCoordinates = firstResponse.mapNotNull { document ->
            val polygon = document.get("polygon", Document::class.java) // Get 'polygon' sub-document
            polygon?.get("coordinates", List::class.java) // Get 'coordinates' as a List
        }[0]


        //polygonCoordinates = listOf(listOf(listOf(7.270287, 51.385495), listOf(7.995289, 51.385495), listOf(7.995289, 51.631657), listOf(7.270287, 51.631657), listOf(7.270287, 51.385495)))

        val secondPipeline = listOf(
            Document(
                "\$match", Document(
                    "\$and", listOf(
                        Document(
                            "timestamp", Document(
                                "\$gte", startDate // Start of the period
                            ).append(
                                "\$lte", endDate // End of the period
                            )
                        ),
                        Document(
                            "location", Document(
                                "\$geoWithin", Document(
                                    "\$geometry", Document("type", "Polygon").append("coordinates", polygonCoordinates)
                                )
                            )
                        ),
                    )
                )
            ),
            Document(
                "\$lookup", Document()
                    .append("from", "flightpoints_ts")
                    .append(
                        "let", Document()
                            .append("f1_flightId", "\$metadata.flightId")
                            .append("f1_timestamp", "\$timestamp")
                    )
                    .append(
                        "pipeline", listOf(
                            Document(
                                "\$match", Document(
                                    "\$expr", Document("\$eq", listOf("\$timestamp", "\$\$f1_timestamp"))
                                )
                            ),
                            // Match stage 2: Match based on the location condition
                            Document(
                                "\$match", Document(
                                    "location", Document(
                                        "\$geoWithin", Document(
                                            "\$geometry", Document("type", "Polygon").append("coordinates", polygonCoordinates)
                                        )
                                    )
                                )
                            ),
                            Document("\$match", Document("\$expr", Document("\$lt", listOf("\$metadata.flightId", "\$\$f1_flightId"))))
                        )
                    )
                    .append("as", "joinedFlights")
            ),
            // Step 4: Unwind the joined flights
            Document("\$unwind", "\$joinedFlights"),
            // Step 5: Project the required fields
            Document(
                "\$project", Document()
                    .append("f1_flightId", "\$metadata.flightId")
                    .append("f2_flightId", "\$joinedFlights.metadata.flightId")
                    .append("timestamp", "\$timestamp")
            ),
            // Step 6: Group results to ensure distinct pairs
            Document(
                "\$group", Document()
                    .append("_id", Document()
                        .append("f1_flightId", "\$f1_flightId")
                        .append("f2_flightId", "\$f2_flightId")
                        .append("timestamp", "\$timestamp")
                    )
            ),
            // Step 7: Sort the results
            Document(
                "\$sort", Document()
                    .append("f1_flightId", 1)
                    .append("f2_flightId", 1)
                    .append("timestamp", 1)
            )
        )


        //println(flightPointsTsCollection.aggregate(secondPipeline).explain(ExplainVerbosity.EXECUTION_STATS))
        val queryTimeStartSecondQuery = Instant.now().toEpochMilli()
        val secondResponse = flightPointsTsCollection.aggregate(secondPipeline).toList()


        return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartSecondQuery, secondResponse)
    }

    fun countFlightsAtInstantInDistricts(
        staticCollections: List<MongoCollection<Document>>,
        dynamicCollections: List<MongoCollection<Document>>,
        instant: String,
    ): Quadrupel<Long, Long, Long, List<Document>>{

        // 2023-01-15 18:00:00 --> detmold 4, Koeln 2, Duesseldrof 8

        val districtsCollection = staticCollections[4]
        val flightPointsTsCollection = dynamicCollections[1]

        val timestamp: Date = dateFormat.parse(instant)

        val firstPipeline = listOf(
            // Step 1: Project only the `name` and `polygon` fields
            Document(
                "\$project", Document()
                    .append("name", 1) // Include the `name` field
                    .append("polygon.coordinates", 1) // Include the `polygon` field
                    .append("_id", 0) // Exclude the `_id` field from the output
            )
        )

        val queryTimeStartFirstQuery = Instant.now().toEpochMilli()
        val firstResponse = districtsCollection.aggregate(firstPipeline).toList()
        val queryEndTimeFirstQuery = Instant.now().toEpochMilli()

        val names = mutableListOf<String>()
        val polygonCoordinates = mutableListOf<List<Any>>()

        for (result in firstResponse) {
            val name = result.getString("name")
            val polygon = result.get("polygon", Document::class.java)
            val coords = polygon?.get("coordinates")

            if (name != null) names.add(name)
            if (coords != null) polygonCoordinates.add(coords as List<Any>) // Cast to the expected type
        }

        val secondPipeline = listOf(

            Document(
                "\$match", Document(
                    "timestamp", timestamp
                )
            ),

            // Step 3: Use $facet to count flights for each polygon
            Document(
                "\$facet", Document().apply {
                    names.forEachIndexed { index, name ->
                        append(name, listOf(
                            Document(
                                "\$match", Document(
                                    "location", Document(
                                        "\$geoWithin", Document(
                                            "\$geometry", Document("type", "Polygon").append("coordinates", polygonCoordinates[index])
                                        )
                                    )
                                )
                            ),
                            Document("\$group", Document("_id", null).append("count", Document("\$sum", 1)))
                        ))
                    }
                }
            ),
            Document(
                "\$project", Document().apply {
                    names.forEach { name ->
                        append(name, Document(
                            "\$ifNull", listOf(
                                Document("\$arrayElemAt", listOf("\$$name.count", 0)),
                                0 // Default to 0 if no documents matched
                            )
                        ))
                    }
                }
            )
        )

        val queryTimeStartSecondQuery = Instant.now().toEpochMilli()
        val secondResponse = flightPointsTsCollection.aggregate(secondPipeline).toList()

        return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartSecondQuery, secondResponse)


    }

    fun inCityRadiusInPeriod(
        staticCollections: List<MongoCollection<Document>>,
        dynamicCollections: List<MongoCollection<Document>>,
        period: List<String>,
        cityName: String,
        radius: Double,
    ): Quadrupel<Long, Long, Long, List<Document>>{

        val firstPipeline = listOf(
            Document("\$match", Document("name", cityName)),
            Document("\$project", Document("location.coordinates", 1).append("_id", 0).append("name", 1))
        )

        val citiesCollection = staticCollections[0]
        val flightPointsTsCollection = dynamicCollections[1]

        val startDate: Date = dateFormat.parse(period[0])
        val endDate: Date = dateFormat.parse(period[1])
        val queryTimeStartFirstQuery = Instant.now().toEpochMilli()

        val firstResponse = citiesCollection.aggregate(firstPipeline).toList()
        val queryEndTimeFirstQuery = Instant.now().toEpochMilli()

        val name = firstResponse.get(0).getString("name")
        val coordinates = firstResponse.flatMap { document ->
            val location = document.get("location", Document::class.java)
            val coords = location?.get("coordinates", List::class.java)
            coords?.map { it as Double } ?: emptyList() // Ensure null-safe mapping
        }

        val secondPipeline = listOf(
            Document(
                "\$match", Document(
                    "\$and", listOf(
                        Document(
                            "timestamp", Document(
                                "\$gte", startDate
                            ).append(
                                "\$lte", endDate
                            )
                        ),
                        Document("location",
                            Document("\$geoWithin",
                                Document("\$centerSphere", listOf(coordinates, radius))
                            )
                        ),
                    )
                )
            ),
            Document(
                "\$group", Document(
                    "_id", Document()
                        .append("flightId", "\$metadata.flightId")
                        .append("track", "\$metadata.track")
                        .append("originAirport", "\$metadata.originAirport")
                        .append("destinationAirport", "\$metadata.destinationAirport")
                        .append("airplaneType", "\$metadata.airplaneType")
                )
            ),
            Document(
                "\$project", Document()
                    .append("flightId", "\$_id.flightId")
                    .append("track", "\$_id.track")
                    .append("originAirport", "\$_id.originAirport")
                    .append("destinationAirport", "\$_id.destinationAirport")
                    .append("airplaneType", "\$_id.airplaneType")
                    .append("_id", 0)
            )
        )

        val queryTimeStartSecondQuery = Instant.now().toEpochMilli()
        val secondResponse = flightPointsTsCollection.aggregate(secondPipeline).toList()
        return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartSecondQuery, secondResponse)
    }

    // takes very long to execute as filtering based on time or space is not possible before calculating distances of each pair of flightpoints
// only way is to create smaller subset collection of flightpoints (but would also take long as check needs to happen only on docs with same timestamp)
    fun closePairOfPlanes(
        staticCollections: List<MongoCollection<Document>>,
        dynamicCollections: List<MongoCollection<Document>>,
        period: List<String>,
        maxDistance: Int
    ): Quadrupel<Long, Long, Long, List<Document>> {

        val flightPointsCollection = dynamicCollections[0]

        val startDate: Date = dateFormat.parse(period[0])
        val endDate: Date = dateFormat.parse(period[1])
        val pipeline = listOf(
            // $lookup stage
            Document(
                "\$lookup", Document()
                    .append("from", "flightpoints")
                    .append("let", Document()
                        .append("point", "\$location")
                        .append("timestamp", "\$timestamp")
                    )
                    .append("pipeline", listOf(
                        Document(
                            "\$geoNear", Document()
                                .append("near", "\$\$point")
                                .append("distanceField", "dist.calculated")
                                .append("maxDistance", maxDistance)
                                .append("query", Document(
                                    "\$and", listOf(
                                        Document("timestamp", "\$\$timestamp"),
                                    )
                                ))
                                .append("key", "location")
                                .append("spherical", true)
                        )
                    ))
                    .append("as", "joinedField")
            ),
            // $match stage
            Document(
                "\$match", Document(
                    "\$expr", Document(
                        "\$gt", listOf(
                            Document("\$size", "\$joinedField"),
                            0
                        )
                    )
                )
            ),
            // $project stage
            Document(
                "\$project", Document()
                    .append("timestamp", 1)
                    .append("flightId", 1)
                    .append("joinedField", 1)
            )
        )

        val queryStartTime = Instant.now().toEpochMilli()
        val firstResponse = flightPointsCollection.aggregate(pipeline).toList()
        val queryEndTimeFirstQuery = Instant.now().toEpochMilli()


        return Quadrupel(queryStartTime, 0, 0, firstResponse)

    }

    fun flightDurationInMunicipalityLowAltitudeInPeriod(
        staticCollections: List<MongoCollection<Document>>,
        dynamicCollections: List<MongoCollection<Document>>,
        period: List<String>,
        municipalityName: String,
        lowAltitude: Int
    ): Quadrupel<Long, Long, Long, List<Document>>{


        val municipalitiesCollection = staticCollections[2]
        val flightPointsTsCollection = dynamicCollections[1]

        val startDate: Date = dateFormat.parse(period[0])
        val endDate: Date = dateFormat.parse(period[1])

        val firstPipeline = listOf(
            Document("\$match", Document("name", municipalityName)),
            Document("\$project", Document("polygon.coordinates", 1).append("_id", 0).append("name", 1))
        )

        val queryTimeStartFirstQuery = Instant.now().toEpochMilli()
        val firstResponse = municipalitiesCollection.aggregate(firstPipeline).toList()
        val queryEndTimeFirstQuery = Instant.now().toEpochMilli()
        //println(firstResponse)
        val name = firstResponse.get(0).getString("name")
        val polygonCoordinates = firstResponse.mapNotNull { document ->
            val polygon = document.get("polygon", Document::class.java) // Get 'polygon' sub-document
            polygon?.get("coordinates", List::class.java) // Get 'coordinates' as a List
        }[0]



        val secondPipeline = listOf(
            Document(
                "\$match", Document(
                    "timestamp", Document()
                        .append("\$gte", startDate) // Start of the period
                        .append("\$lte", endDate) // End of the period
                )
            ),
            Document(
                "\$match", Document(
                    "location", Document(
                        "\$geoWithin", Document(
                            "\$geometry", Document()
                                .append("type", "Polygon")
                                .append("coordinates", polygonCoordinates)
                        )
                    )
                )
            ),
            Document(
                "\$group", Document()
                    .append("_id", Document()
                        .append("flightId", "\$metadata.flightId")
                        .append("originAirport", "\$metadata.originAirport")
                        .append("destinationAirport", "\$metadata.destinationAirport")
                        .append("airplaneType", "\$metadata.airplaneType")
                    )
                    .append(
                        "timestamps", Document(
                            "\$push", Document()
                                .append("timestamp", "\$timestamp")
                                .append("altitude", "\$altitude")
                        )
                    )
            ),
            Document(
                "\$project", Document()
                    .append(
                        "totalTimeBelowAltitude", Document(
                            "\$reduce", Document()
                                .append("input", "\$timestamps")
                                .append("initialValue", Document().append("total", 0).append("prev_altitude", null).append("prev_timestamp", null))
                                .append("in", Document().append("\$cond", listOf(
                                    Document("\$and", listOf(
                                        Document("\$ne", listOf("\$\$value.prev_altitude", 0)), // Previous altitude is not 0
                                        Document("\$ne", listOf("\$\$value.prev_timestamp", 0)), // Previous time is not 0
                                        Document("\$ne", listOf("\$\$value.prev_altitude", null)), // Previous time is not 0
                                        Document("\$ne", listOf("\$\$value.prev_timestamp", null)), // Previous time is not 0
                                        Document("\$lt", listOf("\$\$this.altitude", lowAltitude)), // Current altitude < lowAltitude
                                        Document("\$lt", listOf("\$\$value.prev_altitude", lowAltitude)) // Previous altitude < lowAltitude
                                    )),
                                    Document("prev_altitude", "\$\$this.altitude").append("prev_timestamp", "\$\$this.timestamp")
                                        .append("total", Document("\$add", listOf(
                                            "\$\$value.total",
                                                Document(
                                                    "\$subtract", listOf(
                                                        Document("\$toLong", "\$\$this.timestamp"),
                                                        Document("\$toLong", "\$\$value.prev_timestamp"),
                                                    )
                                                )
                                        ))),
                                    Document("prev_altitude", "\$\$this.altitude").append("prev_timestamp", "\$\$this.timestamp").append("total", "\$\$value.total")

                                )))

                        ))
            ),

            Document(
                "\$project", Document("_id", 0)
                    .append("flightId", "\$_id.flightId")
                    .append("originAirport", "\$_id.originAirport")
                    .append("destinationAirport", "\$_id.destinationAirport")
                    .append("airplaneType", "\$_id.airplaneType")
                    .append("totalTimeBelowAltitude", "\$totalTimeBelowAltitude.total") // Extract the total time
            )
        )

        val queryTimeStartSecondQuery = Instant.now().toEpochMilli()
        val secondResponse = flightPointsTsCollection.aggregate(secondPipeline).toList()
        return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartSecondQuery, secondResponse)

    }

    fun averageHourlyFlightsDuringDayInCounty(
        staticCollections: List<MongoCollection<Document>>,
        dynamicCollections: List<MongoCollection<Document>>,
        day: String,
        countyName: String
    ): Quadrupel<Long, Long, Long, List<Document>>{

        val countiesCollection = staticCollections[3]
        val flightPointsTsCollection = dynamicCollections[1]

        val startDate: Date = dateFormat.parse("$day 00:00:00")
        val endDate: Date = dateFormat.parse("$day 23:59:59")

        val firstPipeline = listOf(
            Document("\$match", Document("name", countyName)),
            Document("\$project", Document("polygon.coordinates", 1).append("_id", 0).append("name", 1))
        )

        val queryTimeStartFirstQuery = Instant.now().toEpochMilli()
        val firstResponse = countiesCollection.aggregate(firstPipeline).toList()
        val queryEndTimeFirstQuery = Instant.now().toEpochMilli()

        val name = firstResponse.get(0).getString("name")
        val polygonCoordinates = firstResponse.mapNotNull { document ->
            val polygon = document.get("polygon", Document::class.java)
            polygon?.get("coordinates", List::class.java)
        }[0]

        val secondPipeline = listOf(
            Document(
                "\$match", Document(
                    "timestamp", Document()
                        .append("\$gte", startDate)
                        .append("\$lte", endDate)
                )
            ),
            Document(
                "\$match", Document(
                    "location", Document(
                        "\$geoWithin", Document(
                            "\$geometry", Document("type", "Polygon").append("coordinates", polygonCoordinates)
                        )
                    )
                )
            ),
            Document(
                "\$project", Document()
                    .append("hour", Document("\$hour", "\$timestamp"))
                    .append("flightId", "\$metadata.flightId")
            ),
            Document(
                "\$group", Document()
                    .append("_id", Document()
                        .append("hour", "\$hour")
                        .append("flightId", "\$flightId")
                    )
                    .append("count", Document("\$sum", 1))
            ),
            Document(
                "\$group", Document()
                    .append("_id", "\$_id.hour")
                    .append("activeFlights", Document("\$sum", 1))
            ),
            Document(
                "\$project", Document()
                    .append("hour", "\$_id")
                    .append("activeFlights", 1)
                    .append("_id", 0)
            ),
            Document(
                "\$sort", Document("hour", 1)
            )
        )

        val queryTimeStartSecondQuery = Instant.now().toEpochMilli()
        val secondResponse = flightPointsTsCollection.aggregate(secondPipeline).toList()
        return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartSecondQuery, secondResponse)

    }


    // doesnt work easily (very complex to implement)
    fun crossedCountiesInPeriodWithDestination(
        staticCollections: List<MongoCollection<Document>>,
        dynamicCollections: List<MongoCollection<Document>>,
        period: List<String>,
        ): Quadrupel<Long, Long, Long, List<Document>>{


        val countiesCollection = staticCollections[3]
        val flightPointsCollection = dynamicCollections[0]
        val destinationAirport = "EDDK"

        val startDate: Date = dateFormat.parse(period[0])
        val endDate: Date = dateFormat.parse(period[1])

        val firstPipeline = listOf(
            Document(
                "\$match", Document(
                    "\$and", listOf(
                            Document("destinationAirport", destinationAirport),
                            Document(
                                "timestamp", Document()
                                    .append("\$gte", startDate) // Start of the period
                                    .append("\$lte", endDate) // End of the period
                            ),
                        ),

                    )
                ),
            Document("\$project", Document("location", 1).append("_id", 0))
        )

        val queryTimeStartFirstQuery = Instant.now().toEpochMilli()
        val pointsDocuments = flightPointsCollection.aggregate(firstPipeline).toList()
        val queryEndTimeFirstQuery = Instant.now().toEpochMilli()


//        if (firstResponse.isNotEmpty()) {
//            val coordinatesList = firstResponse.map { document ->
//                val coordinates = document["coordinates"] as List<*>
//                // Safely cast the elements to Double
//                listOf(
//                    (coordinates[0] as Number).toDouble(), // Longitude
//                    (coordinates[1] as Number).toDouble()  // Latitude
//                )
//            }
//        }



        val secondPipeline = listOf(
            Document(
                "\$match", Document(
                    "\$or", pointsDocuments.map { pointDocument ->
                        Document(
                            "polygon", Document(
                                "\$geoIntersects", Document(
                                    "\$geometry", pointDocument["location"]
                                )
                            )
                        )
                    }
                )
            ),
            Document(
                "\$project", Document(
                    "_id", 0
                ).append(
                    "name", 1
                )
            )
        )

        val queryTimeStartSecondQuery = Instant.now().toEpochMilli()

        var secondResponse = emptyList<Document>()

        if(pointsDocuments.isNotEmpty()) {
            secondResponse = countiesCollection.aggregate(secondPipeline).toList()
        }

        return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartSecondQuery, secondResponse)

    }


    fun flightsWithLocalOriginDestinationInPeriodInCounty(
        staticCollections: List<MongoCollection<Document>>,
        dynamicCollections: List<MongoCollection<Document>>,
        period: List<String>,
        countyName: String
    ): Quadrupel<Long, Long, Long, List<Document>>{

        val airportsCollection = staticCollections[1]

        val startDate: Date = dateFormat.parse(period[0])
        val endDate: Date = dateFormat.parse(period[1])
        val countiesCollection = staticCollections[3]

        val firstPipeline = listOf(
            Document("\$match", Document("name", countyName)),
            Document("\$project", Document("polygon.coordinates", 1).append("_id", 0).append("name", 1))
        )

        val queryTimeStartFirstQuery = Instant.now().toEpochMilli()
        val firstResponse = countiesCollection.aggregate(firstPipeline).toList()
        val queryEndTimeFirstQuery = Instant.now().toEpochMilli()

        val name = firstResponse.get(0).getString("name")
        val polygonCoordinates = firstResponse.mapNotNull { document ->
            val polygon = document.get("polygon", Document::class.java)
            polygon?.get("coordinates", List::class.java)
        }[0]


        val pipeline = listOf(
            // Stage 1: Lookup cities for airports
            Document(
                "\$lookup", Document("from", "cities")
                    .append("localField", "City")
                    .append("foreignField", "name")
                    .append("as", "cityDetails")
            ),
            Document("\$match", Document("cityDetails", Document("\$ne", emptyList<Any>()))),
            Document("\$project", Document("ICAO", 1)
                .append("cityName", Document("\$arrayElemAt", listOf("\$cityDetails.name", 0)))
                .append("_id", 0)),

            // Stage 2: Group local airports into a list
            Document("\$group", Document("_id", null)
                .append("localAirports", Document("\$addToSet", "\$ICAO"))),

            // Stage 3: Pass localAirports list to the lookup
            Document("\$lookup", Document("from", "flightpoints_ts")
                .append("let", Document("localAirports", "\$localAirports"))
                .append("pipeline", listOf(

                    Document(
                        "\$match", Document(
                            "\$and", listOf(
                                Document(
                                    "timestamp", Document( // Access the timestamp field inside points
                                        "\$gte", startDate
                                    ).append(
                                        "\$lte", endDate
                                    )
                                ),
                                Document(
                                    "location", Document( // Access the location field inside points
                                        "\$geoWithin", Document(
                                            "\$geometry", Document("type", "Polygon").append("coordinates", polygonCoordinates)
                                        )
                                    )
                                ),
                            )
                        )
                    ),
                    Document(
                        "\$group", Document()
                            .append(
                                "_id", Document(
                                    "flightId", "\$metadata.flightId"
                                ).append(
                                    "track", "\$metadata.track"
                                )
                            ) // Group by flightId and track
                            .append("airplaneType", Document("\$first", "\$metadata.airplaneType"))
                            .append("destinationAirport", Document("\$first", "\$metadata.destinationAirport"))
                            .append("originAirport", Document("\$first", "\$metadata.originAirport"))
                    ),

                    Document(
                        "\$match", Document("\$expr", Document("\$or", listOf(
                            Document("\$in", listOf("\$originAirport", "\$\$localAirports")),
                            Document("\$in", listOf("\$destinationAirport", "\$\$localAirports"))
                        )))
                    )
                ))
                .append("as", "filteredFlightTrips")
            ),
            Document("\$unwind", "\$filteredFlightTrips"),
            Document(
                "\$lookup", Document("from", "airports")
                    .append("localField", "filteredFlightTrips.originAirport")
                    .append("foreignField", "ICAO")
                    .append("as", "cityNameOrigin")
            ),
            Document(
                "\$lookup", Document("from", "airports")
                    .append("localField", "filteredFlightTrips.destinationAirport")
                    .append("foreignField", "ICAO")
                    .append("as", "cityNameDestination")
            ),
            // Stage 4: Project final fields
            Document("\$project", Document()
                .append("_id", 0)
                .append("flightId", "\$filteredFlightTrips.flightId")
                .append("originAirport", "\$filteredFlightTrips.originAirport")
                .append("destinationAirport", "\$filteredFlightTrips.destinationAirport")
                .append("originCity", "\$cityNameOrigin.City")
                .append("destinationCity", "\$cityNameDestination.City")
                .append("airplaneType", "\$filteredFlightTrips.airplaneType")
            )
        )

        val queryTimeStart = Instant.now().toEpochMilli()

        return Quadrupel(queryTimeStart, 0, 0, airportsCollection.aggregate(pipeline).toList())
    }


}






