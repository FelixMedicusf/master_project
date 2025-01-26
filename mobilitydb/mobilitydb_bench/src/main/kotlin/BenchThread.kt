import java.io.File
import java.io.PrintStream
import java.lang.Thread.UncaughtExceptionHandler
import java.sql.Connection
import java.sql.DriverManager.getConnection
import java.sql.ResultSet
import java.sql.Statement
import java.text.DecimalFormat
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
    private val queryQueue: ConcurrentLinkedQueue<QueryTask>,
    private val log: MutableList<QueryExecutionLog>,
    private val startLatch: CountDownLatch,
    private val seed: Long
) : Thread(threadName) {


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
    }

    override fun run() {
        var connection: Connection? = null
        var statement: Statement? = null

        val logFile = File("sql_response_log_${threadName}.txt")
        if (logFile.exists()) {
            logFile.delete()
        }
        logFile.createNewFile()

        val printStream = PrintStream(logFile)
        try {
            // File to capture the output


            connection = getConnection(
                "jdbc:postgresql://$mobilityDBIp/$DATABASE", USER, PASSWORD
            )

            statement = connection.createStatement()

            if (threadName == "thread-0"){
                statement.executeUpdate("SET citus.max_intermediate_result_size TO '12GB';")
                statement.executeUpdate("ALTER DATABASE $DATABASE SET default_transaction_read_only = on;")
                statement.execute("SET autovacuum = off;");
                statement.execute("SET checkpoint_timeout = '1h';");

            }
            // Ensure all threads start at the same time

            startLatch.await()

            printStream.println("$threadName started executing at ${Instant.now()} with ${queryQueue.size} queries.")
            println()
            var i = 1

            println("$threadName started executing at ${Instant.now()}.")
            while (true) {
                val task = queryQueue.poll() ?: break


                val sqlQuery = if (task.paramSet != null) {
                    replaceParams(task.sql, task.paramSet)

                    //this.formatSQLStatement(task.sql, task.params)
                } else {
                   task.sql
                }

                val startTime = Instant.now().toEpochMilli()
                printStream.println("$i: $sqlQuery")
                val response = statement.executeQuery(sqlQuery)
                val endTime = Instant.now().toEpochMilli()
                val params = task.paramSet?.keys?.joinToString(";") ?: ""
                val parameterValues = task.paramSet?.values?.joinToString(";") ?: ""


                response.use { resultSet ->
                    // Get metadata to print column headers
                    val metaData = resultSet.metaData
                    val columnCount = metaData.columnCount

                    // Print column headers
                    for (i in 1..columnCount) {
                        printStream.print("${metaData.getColumnName(i)}\t") // Tab-separated for better readability
                    }
                    printStream.println() // Move to the next line after printing column headers

                    // Print all rows
                    while (resultSet.next()) {
                        for (i in 1..columnCount) {
                            val value = resultSet.getString(i) ?: "NULL" // Handle NULL values
                            printStream.print("$value\t")
                        }
                        printStream.println() // Move to the next line after each row
                    }

                    printStream.println() // Add an extra line for separation after the response
                }


                synchronized(log) {
                    log.add(
                        QueryExecutionLog(
                            threadName = threadName,
                            queryName = task.queryName,
                            queryType = task.type,
                            params = params,
                            paramValues = parameterValues.replace(",", "/"),
                            round = 0,
                            executionIndex = 0,
                            startTime = startTime,
                            endTime = endTime,
                            startTimeSecond = 0,
                            endTimeSecond = 0,
                            latency = (endTime - startTime),
                            records = -1
                        )
                    )
                }

                i++
            }

            printStream.println("$threadName finished executing at ${Instant.now()}.")
            println("$threadName finished executing at ${Instant.now()}.")

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

    private fun replaceParams(sql: String, paramSet: Map<String, String>): String {
        var parsedsql = sql

        for ((key, value) in paramSet) {
            parsedsql = parsedsql.replace(":${key}", value)
        }
        return parsedsql
    }


    private fun formatSQLStatement(sql: String, params: List<String>): Pair<String, List<String>> {
        var parsedSql = sql
        var values = mutableListOf<String>()

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
                "radius" -> ((random.nextDouble(0.25, 0.5) * 10)).toString(); // /6378.1
                "low_altitude" -> (random.nextInt(300, 600) * 10).toString();
                "type" -> "'${airplanetypes[random.nextInt(0, airplanetypes.size)]}'"
                "distance" -> (random.nextInt(10, 100) * 10).toString() // in meter in MongoDB
                else -> ""

            }

            if (replacement != null){
                parsedSql = parsedSql.replace(":$param", replacement)
                values.add(replacement)

            }

        }
        return Pair(parsedSql, values)
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
                // Between 2 days and 15 days
                val secondsToShift = random.nextLong(172800, 1296001)
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
                // Between 15 days and 12 months
                val secondsToShift = random.nextLong(1296000, 31536001)
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

    private fun getRandomPoint(random: Random, rectangle: List<List<Double>>):String {
        val upperLeftLon = rectangle[0][0]
        val upperLeftLat = rectangle[0][1]
        val bottomRightLon = rectangle[1][0]
        val bottomRightLat = rectangle[1][1]

        val randomLon = upperLeftLon + random.nextDouble() * (bottomRightLon - upperLeftLon)

        val randomLat = bottomRightLat + random.nextDouble() * (upperLeftLat - bottomRightLat)

        // Return the random point
        return "$randomLon $randomLat"
    }

    private fun getRandomDay(random: Random, year: Int): String {

        val startDate = LocalDate.of(year, 1, 1)
        val endDate = LocalDate.of(year, 12, 31)

        val daysInYear = endDate.toEpochDay() - startDate.toEpochDay() + 1

        val randomDay = startDate.plusDays(random.nextLong(0, daysInYear))

        return "'${randomDay}'"
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
            // Between 2 days and 15 days
            val secondsToShift = random.nextLong(172800, 1296001)
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
            // Between 15 days and 12 months
            val secondsToShift = random.nextLong(1296000, 31536001)
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

    val (start, end) = if (timestamp1.isBefore(endDate)) {
        timestamp1 to endDate
    } else {
        endDate to timestamp1
    }

    return "tstzspan'[${start.format(formatter)}, ${end.format(formatter)}]'"
}


fun parseCSV(filePath: String, requiredColumns: Set<String>): List<Map<String, String>> {
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

private fun getRandomPoint(random: Random, rectangle: List<List<Double>>):String {
    val upperLeftLon = rectangle[0][0]
    val upperLeftLat = rectangle[0][1]
    val bottomRightLon = rectangle[1][0]
    val bottomRightLat = rectangle[1][1]

    val randomLon = upperLeftLon + random.nextDouble() * (bottomRightLon - upperLeftLon)

    val randomLat = bottomRightLat + random.nextDouble() * (upperLeftLat - bottomRightLat)

    // Return the random point
    return "$randomLon $randomLat"
}

private fun getRandomDay(random: Random, year: Int): String {

    val startDate = LocalDate.of(year, 1, 1)
    val endDate = LocalDate.of(year, 12, 31)

    val daysInYear = endDate.toEpochDay() - startDate.toEpochDay() + 1

    val randomDay = startDate.plusDays(random.nextLong(0, daysInYear))

    return "'${randomDay}'"
}

fun main(){



    val random = Random(12345)
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val countiesPath = "src/main/resources/counties.csv"
    val districtsPath = "src/main/resources/districts.csv"
    val municipalitiesPath = "src/main/resources/municipalities.csv"
    val citiesPath = "src/main/resources/cities.csv"
    val airportsPath = "src/main/resources/airports.csv"
    val airplanetypesPath = "src/main/resources/airplanetypes.csv"


    val municipalities = parseCSV(municipalitiesPath, setOf("name"))
    val counties = parseCSV(countiesPath, setOf("name"))
    val districts = parseCSV(districtsPath, setOf("name"))
    val cities = parseCSV(citiesPath, setOf("area", "lat", "lon", "district", "name", "population"))
    val airports = parseCSV(airportsPath, setOf("IATA", "ICAO", "Airport name", "Country", "City"))
    val airplanetypes = File(airplanetypesPath).readLines()


    println(generateParameterSets(5, 2023))

//    generateRandomTimeSpan(random=random, formatter = formatter, year=2023, mode=1)
//    generateRandomTimeSpan(random=random, formatter = formatter, year=2023, mode=2)
//    generateRandomTimeSpan(random=random, formatter = formatter, year=2023, mode=3)
//    generateRandomTimeSpan(formatter = formatter, year=2023, random = random)
//    generateRandomTimestamp(formatter = formatter, random = random)
//    getRandomDay(random = random, year = 2023)
//    getRandomPlace(cities, "name", random)
//    getRandomPlace(airports, "Airport name", random)
//    getRandomPlace(municipalities, "name", random)
//    getRandomPlace(counties, "name", random)
//    getRandomPlace(districts, "name", random)
//    getRandomPoint(random, listOf(listOf(6.212909, 52.241256), listOf(8.752841, 50.53438)))
    //((random.nextDouble(0.25, 0.5) * 10)).toString(); // /6378.1
    //(random.nextInt(300, 600) * 10).toString();
    //"'${airplanetypes[random.nextInt(0, airplanetypes.size)]}'"
    //(random.nextInt(10, 100) * 10).toString() // in meter in MongoDB
}

fun generateParameterSets(entryCount: Int, year: Int): String {


    val countiesPath = "src/main/resources/counties.csv"
    val districtsPath = "src/main/resources/districts.csv"
    val municipalitiesPath = "src/main/resources/municipalities.csv"
    val citiesPath = "src/main/resources/cities.csv"
    val airportsPath = "src/main/resources/airports.csv"
    val airplanetypesPath = "src/main/resources/airplanetypes.csv"


    val municipalities = parseCSV(municipalitiesPath, setOf("name"))
    val counties = parseCSV(countiesPath, setOf("name"))
    val districts = parseCSV(districtsPath, setOf("name"))
    val cities = parseCSV(citiesPath, setOf("area", "lat", "lon", "district", "name", "population"))
    val airports = parseCSV(airportsPath, setOf("IATA", "ICAO", "Airport name", "Country", "City"))

    val random = Random(System.currentTimeMillis())
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")


        val parameterSets = (1..entryCount).joinToString("\n") { index ->
            //val period = generateRandomTimeSpan(random, formatter, 2023, 2)
            val municipality = getRandomPlace(municipalities, "name", random)
            val county = getRandomPlace(counties, "name", random)
            val district = getRandomPlace(districts, "name", random)
            val radius = ((random.nextDouble(0.25, 0.5) * 10))
            val city = getRandomPlace(cities, "name", random)
            val altitude = (random.nextInt(100, 200) * 10).toString();
            val period = generateRandomTimeSpan(random, formatter, 2023, 0)
            val distance = (random.nextInt(10, 100) * 10).toString()
            val point = getRandomPoint(random, listOf(listOf(6.212909, 52.241256), listOf(8.752841, 50.53438)))
            val instant = generateRandomTimestamp(random, formatter)
            val day = getRandomDay(random, 2023)
            val df = DecimalFormat("#.##########")
            val radian_radius = radius/6378.1
            val degree_radius = radian_radius*57.2958

            """
            - parameters:
                radius: ${df.format(radian_radius)}
                radius: ${df.format(degree_radius)}
            """.trimIndent()
    }

    return """
    parameter_sets:
    $parameterSets
    """.trimIndent()
}


