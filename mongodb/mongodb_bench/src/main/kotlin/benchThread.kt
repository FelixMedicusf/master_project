import java.io.File
import java.sql.Connection
import java.sql.DriverManager.getConnection
import java.sql.ResultSet
import java.sql.Statement
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import kotlin.random.Random

class BenchThread(
    private val threadName: String,
    private val mobilityDBIp: String,
    private val databaseName: String,
    private val user: String,
    private val password: String,
    private val queryQueue: ConcurrentLinkedQueue<QueryTask>,
    private val log: MutableList<QueryExecutionLog>,
    private val startLatch: CountDownLatch
) : Thread(threadName) {

    private val seed = 12345L
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
    }

    override fun run() {
        var connection: Connection? = null
        var statement: Statement? = null
        try {

            connection = getConnection(
                "jdbc:postgresql://$mobilityDBIp/$databaseName", user, password
            )

            statement = connection.createStatement()


            // Ensure all threads start at the same time
            startLatch.await()

            println("$threadName started executing at ${Instant.now()}.")
            println()

            while (true) {
                val task = queryQueue.poll() ?: break

                val sqlQuery = if (task.params != null) {
                    this.formatSQLStatement(task.sql, task.params)
                } else {
                    task.sql
                }


                val startTime = Instant.now().toEpochMilli()

                val response = statement.executeQuery(sqlQuery)
                val endTime = Instant.now().toEpochMilli()

                println(sqlQuery)
                printSQLResponse(response)

                synchronized(log) {
                    log.add(
                        QueryExecutionLog(
                            threadName = threadName,
                            queryName = task.queryName,
                            queryType = task.type,
                            round = 0,
                            executionIndex = 0,
                            startTime = startTime,
                            endTime = endTime,
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

            try {
                statement?.close()
            } catch (e: Exception) {
                println("Failed to close statement: ${e.message}")
            }

            try {
                connection?.close()
            } catch (e: Exception) {
                println("Failed to close connection: ${e.message}")
            }
        }

    }


    private fun formatSQLStatement(sql: String, params: List<String>): String {
        var parsedSql = sql

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

            if (replacement != null)parsedSql = parsedSql.replace(":$param", replacement)

        }
        return parsedSql
    }

    private fun printSQLResponse(resultSet: ResultSet) {
        try {

            val metaData = resultSet.metaData
            val columnCount = metaData.columnCount

            // Print column names
            for (i in 1..columnCount) {
                print("${metaData.getColumnName(i)}\t")
            }
            println()


            while (resultSet.next()) {
                for (i in 1..columnCount) {
                    print("${resultSet.getString(i)}\t")
                }
                println()
            }

            println()

        } catch (e: Exception) {
            println("Error while processing ResultSet: ${e.message}")
            e.printStackTrace()
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
    private fun generateRandomTimeSpan(random: Random, formatter: DateTimeFormatter, year: Int, mode: Int = 0): String {

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

        return "tstzspan'[${start.format(formatter)}, ${end.format(formatter)}]'"
    }


    private fun parseCSV(filePath: String, requiredColumns: Set<String>): List<Map<String, String>> {
        val rows = mutableSetOf<Map<String, String>>()
        val lines = File(filePath).readLines()

        if (lines.isNotEmpty()) {
            val header = lines.first().split(",") // Get column headers

            val indicesToKeep = header.withIndex()
                .filter { it.value in requiredColumns }
                .map { it.index }

            rows.addAll(
                lines.drop(1).map { line ->
                    val values = line.split(",")
                    indicesToKeep.associate { header[it] to values[it] }
                }.distinct() // Ensure distinct values during map creation
            )
        }

        return rows.toList() // Convert back to a List to match return type
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

        return "'${randomDay}'"
    }

}

