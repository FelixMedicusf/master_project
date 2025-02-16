import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.KotlinFeature
import com.fasterxml.jackson.module.kotlin.KotlinModule
import dfsData.DFSDataHandler
import io.ktor.http.*
import io.ktor.serialization.jackson.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import java.util.concurrent.*
import kotlin.random.Random
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.plugins.statuspages.*
import java.sql.DriverManager.getConnection
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

const val DATABASE = "aviation_data"
const val USER = "felix"
const val PASSWORD = "master"
var benchmarkExecutorService: ExecutorService? = null
val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

class BenchmarkExecutor(
    private val configPath: String,
    private val logsPath: String,
) {

    private val mapper: ObjectMapper = ObjectMapper(YAMLFactory()).apply {
        registerModule(
            KotlinModule.Builder()
                .withReflectionCacheSize(512)
                .configure(KotlinFeature.NullToEmptyCollection, false)
                .configure(KotlinFeature.NullToEmptyMap, false)
                .configure(KotlinFeature.NullIsSameAsDefault, false)
                .configure(KotlinFeature.SingletonSupport, false)
                .configure(KotlinFeature.StrictNullChecks, false)
                .build()
        )
        configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true)
        configure(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE, true)
        configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    }

    fun execute() {
        val config = loadConfig() ?: return

        val threadCount = config.benchmarkSettings.threads
        val nodes = config.benchmarkSettings.nodes
        val sut = config.benchmarkSettings.sut
        val mainSeed = config.benchmarkSettings.randomSeed
        val mixed = config.benchmarkSettings.mixed
        val distributed = config.benchmarkSettings.nodes.size > 1
        val logResponses = config.benchmarkSettings.test
        val mainRandom = Random(mainSeed)
        val warmUpRandom = Random(12345)
        println("Using random seed: $mainSeed")
        println("Using $threadCount threads.")
        println("Mixed queries: $mixed")
        println("Distributed: $distributed")

        val allQueries = prepareQueryTasks(config, mainRandom)
        val executionLogs = Collections.synchronizedList(mutableListOf<QueryExecutionLog>())
        val threadSafeQueries = ConcurrentLinkedQueue(allQueries)
        val startLatch = CountDownLatch(1)

        warmUpSut(nodes[0], 200, warmUpRandom)

        val benchThreads = Executors.newFixedThreadPool(threadCount)
        val threadSeeds = generateRandomSeeds(mainRandom, threadCount)
        for (i in 0..<threadCount) {
            benchThreads.submit(
                BenchThread(
                    "thread-$i", nodes[0], threadSafeQueries, executionLogs, startLatch, logResponses
                )
            )

        }

        println("Releasing $threadCount threads to start execution.")
        val benchStart = Instant.now().toEpochMilli()
        startLatch.countDown()

        benchThreads.shutdown()
        benchThreads.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)
        val benchEnd = Instant.now().toEpochMilli()


        if (logResponses){
            mergeLogFilesAndCleanUp("sql_response_log_combined.txt", "sql_response_log_.*\\.txt")
        }

        saveExecutionLogs(threadSeeds, executionLogs, benchStart, benchEnd, nodes.size, sut, mainSeed)
    }

    private fun warmUpSut(mobilityDBIp: String, repetitions: Int = 50, warmUpRandom: Random){
        val connection = getConnection(
            "jdbc:postgresql://$mobilityDBIp/$DATABASE", USER, PASSWORD
        )

        val statement = connection.createStatement()
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        var i = 0


        println("Starting warm Up phase.")
        try {
            while (i < repetitions){

                val randomFlightId = warmUpRandom.nextLong(691877560, 774441640)
                val randomMunicipality = getRandomPlace(municipalities, "name", warmUpRandom)
                val randomCounty = getRandomPlace(counties, "name", warmUpRandom)
                val randomDistrict = getRandomPlace(districts, "name", warmUpRandom)
                val randomCity = getRandomPlace(cities, "name", warmUpRandom)
                val randomTimespan = generateRandomTimeSpan(warmUpRandom, formatter, 2023, 2)

                statement.executeQuery("SELECT * FROM flights WHERE flightid=$randomFlightId")
                statement.executeQuery("SELECT * FROM flightpoints WHERE flightid=$randomFlightId")
                statement.executeQuery("SELECT * FROM counties WHERE name=$randomCounty")
                statement.executeQuery("SELECT * FROM municipalities WHERE name=$randomMunicipality")
                statement.executeQuery("SELECT * FROM districts WHERE name=$randomDistrict")
                statement.executeQuery("SELECT * FROM cities WHERE name=$randomCity")

                statement.executeQuery("SELECT flightid, track, destination FROM flights f, counties c WHERE f.trip && stbox(c.geom, $randomTimespan) AND c.name = $randomCounty LIMIT 5")
                statement.executeQuery("SELECT flightid, track FROM flights WHERE trip && $randomTimespan")
                i++

            }
        } catch (e: Exception) {
            e.printStackTrace()
        } finally {
            println("Finished warm up phase of SUT.")
            connection.close()
        }


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

    private fun loadConfig(): BenchmarkConfiguration? {
        return try {
            Files.newBufferedReader(Paths.get(configPath)).use { bufferedReader ->
                mapper.readValue(bufferedReader, BenchmarkConfiguration::class.java)
            }
        } catch (e: Exception) {
            println("Error reading or parsing configuration file: ${e.message}")
            null
        }
    }


    private fun prepareQueryTasks(config: BenchmarkConfiguration, random: Random): MutableList<QueryTask> {
        val allQueries = mutableListOf<QueryTask>()
        for (queryConfig in config.queryConfigs) {
            if (queryConfig.use) {
                repeat(queryConfig.repetition){
                    val parsedSqlAndValues = formatSQLStatement(queryConfig.sql, queryConfig.parameters, random)
                    allQueries.add(QueryTask(queryConfig.name, queryConfig.type, parsedSqlAndValues.first, parsedSqlAndValues.second))
                }
            }
        }
        if(config.benchmarkSettings.mixed){
            allQueries.shuffle(random)
        }
        return allQueries
    }

    private fun generateRandomSeeds(mainRandom: Random, threadCount: Int): List<Long> {
        val seeds = mutableListOf<Long>()
        for (i in 0..<threadCount) {
            seeds.add(mainRandom.nextLong())
        }
        return seeds
    }

    private fun saveExecutionLogs(threadSeeds: List<Long>, executionLogs: List<QueryExecutionLog>, benchStart: Long, benchEnd: Long, nodeNumber: Int, sut: String, mainSeed: Long) {

        val file = File(logsPath)
        file.writeText("start: ${Date(benchStart)}, end: ${Date(benchEnd)}, duration (s): ${(benchEnd - benchStart)/1000}. " + "SUT: $sut, " + "#threads: ${threadSeeds.size},"  + " #nodes: $nodeNumber, queries executed: ${executionLogs.size}. Seed: $mainSeed" + "\n")
        file.appendText("threadName,queryName,queryType,parameterValues,round,executionIndex,startFirstQuery,endFirstQuery,startSecQuery,endSecQuery,latency,fetchedRecords\n")
        file.appendText(executionLogs.joinToString(separator = "\n"))
        println("Execution logs have been written to $logsPath")
    }


    private fun formatSQLStatement(sql: String, params: List<String>, random: Random): Pair<String, List<String>> {
        var parsedSql = sql
        var values = mutableListOf<String>()

        for (param in params){
            val replacement = when (param) {
                "period_short" -> generateRandomTimeSpan(random, formatter, year=2023, mode=1)
                "period_medium" -> generateRandomTimeSpan(random, formatter, year=2023, mode=2)
                "period_long" -> generateRandomTimeSpan(random, formatter, year=2023, mode=3)
                "period" -> generateRandomTimeSpan(random, formatter, year=2023)
                "instant" -> generateRandomTimestamp(random, formatter)
                "day" -> getRandomDay(random, year = 2023)
                "city" -> getRandomPlace(cities, "name", random)
                "municipality" -> getRandomPlace(municipalities, "name", random)
                "county" -> getRandomPlace(counties, "name", random)
                "district" -> getRandomPlace(districts, "name", random)
                "point" -> getRandomPoint(random, listOf(listOf(6.212909, 52.241256), listOf(8.752841, 50.53438)))
                "radius" -> ((random.nextInt(2, 10) * 10)).toString(); // /6378.1
                "low_altitude" -> (random.nextInt(50, 150) * 10).toString();
                "distance" -> (random.nextInt(1, 10)) .toString() // in meter in MongoDB
                else -> ""

            }

            if (replacement != null){
                parsedSql = parsedSql.replace(":$param", replacement)
                values.add(replacement)

            }

        }
        return Pair(parsedSql, values)
    }

    private fun getRandomPoint(random: Random, rectangle: List<List<Double>>):String {
        val upperLeftLon = rectangle[0][0]
        val upperLeftLat = rectangle[0][1]
        val bottomRightLon = rectangle[1][0]
        val bottomRightLat = rectangle[1][1]

        val randomLon = upperLeftLon + random.nextDouble() * (bottomRightLon - upperLeftLon)

        val randomLat = bottomRightLat + random.nextDouble() * (upperLeftLat - bottomRightLat)

        // Return the random point
        return "POINT($randomLon $randomLat)"
    }

    private fun getRandomDay(random: Random, year: Int): String {

        val startDate = LocalDate.of(year, 1, 1)
        val endDate = LocalDate.of(year, 12, 31)

        val daysInYear = endDate.toEpochDay() - startDate.toEpochDay() + 1

        val randomDay = startDate.plusDays(random.nextLong(0, daysInYear))

        return "'${randomDay}'"
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


    fun mergeLogFilesAndCleanUp(outputFileName: String, logFilePattern: String) {
        val outputFile = File(outputFileName)

        // Delete the output file if it already exists
        if (outputFile.exists()) {
            outputFile.delete()
        }

        // Find all thread-specific log files matching the pattern
        val logFiles = File(".").listFiles { _, name ->
            name.matches(Regex(logFilePattern))
        } ?: emptyArray()

        // Merge content into the output file
        outputFile.bufferedWriter().use { writer ->
            for (logFile in logFiles) {
                writer.appendLine("=== Content from ${logFile.name} ===")
                logFile.forEachLine { line ->
                    writer.appendLine(line)
                }
                writer.appendLine() // Add a blank line between files
            }
        }

        // Delete the individual thread log files
        logFiles.forEach { it.delete() }

        println("Merged ${logFiles.size} log files into ${outputFile.absolutePath}. Deleted thread-specific log files.")
    }

}


fun main() {
    // Path to the config and logs
    val configPath = "benchConf.yaml"
    val logsPath = "sql_benchmark_execution_logs.txt"
    val responsesPath = "sql_response_log_combined.txt"

    // Start HTTP server
    embeddedServer(Netty, port = 8080) {
        install(ContentNegotiation) {
            jackson {}
        }
        install(StatusPages) {
            exception<Throwable> { call, cause ->
                cause.printStackTrace()
                call.respond(
                    HttpStatusCode.InternalServerError,
                    "An internal server error occurred: ${cause.message}"
                )
            }
        }
        routing {
            println("Started Benchmarking Server v1.0")
            post("/data-handler") {
                try {
                    val handler = DFSDataHandler(DATABASE)

                    // Perform DataHandler operations sequentially
                    try {
                        handler.processStaticData()
                        handler.insertFlightPoints()
                        handler.interpolateFlightPoints()
                        handler.createGeographies()
                        handler.createFlightTrips()
                        handler.createIndexes()

                        println("DataHandler operations completed successfully.")
                        call.respond(HttpStatusCode.OK, "DataHandler operations completed successfully.")
                    } catch (e: Exception) {
                        e.printStackTrace()
                        println("Error during DataHandler operations: ${e.message}")
                        call.respond(
                            HttpStatusCode.InternalServerError,
                            "Error during DataHandler operations: ${e.message}"
                        )
                    }
                } catch (e: Exception) {
                    e.printStackTrace()
                    call.respond(
                        HttpStatusCode.InternalServerError,
                        "Error starting DataHandler operations: ${e.message}"
                    )
                }
            }


            // Start benchmark execution
            post("/start-benchmark") {
                if (benchmarkExecutorService != null && !benchmarkExecutorService!!.isShutdown) {
                    call.respond(HttpStatusCode.BadRequest, "Benchmark execution is already running.")
                    return@post
                }

                println("Received request to start the benchmark execution.")
                // Initialize executor service with custom ThreadFactory
                benchmarkExecutorService = Executors.newSingleThreadExecutor { runnable ->
                    Thread(runnable).apply {
                        uncaughtExceptionHandler = Thread.UncaughtExceptionHandler { _, exception ->
                            exception.printStackTrace() // Log the exception to the console
                        }
                    }
                }

                benchmarkExecutorService!!.submit {
                    try {
                        val executor = BenchmarkExecutor(configPath, logsPath)
                        executor.execute()
                    } catch (e: Exception) {
                        e.printStackTrace() // Log any unexpected exceptions from execute()
                    }
                }
                call.respond(HttpStatusCode.OK, "Benchmark execution started.")
            }

            // Stop benchmark execution
            post("/stop-benchmark") {
                if (benchmarkExecutorService == null || benchmarkExecutorService!!.isShutdown) {
                    call.respond(HttpStatusCode.BadRequest, "No benchmark execution is running.")
                    return@post
                }

                benchmarkExecutorService!!.shutdownNow()
                call.respond(HttpStatusCode.OK, "Benchmark execution stopped.")
            }

            // Upload YAML configuration file
            post("/upload-config") {
                println("Uploading benchmark configurations.")
                val configFileBytes = call.receive<ByteArray>()
                File(configPath).writeBytes(configFileBytes)
                call.respond(HttpStatusCode.OK, "Configuration file uploaded.")
            }

            // Retrieve benchmark logs
            get("/retrieve-logs") {
                val logFile = File(logsPath)
                if (logFile.exists()) {
                    call.respondFile(logFile)
                } else {
                    call.respond(HttpStatusCode.NotFound, "Log file not found.")
                }
            }

            get("/retrieve-responses") {
                val responseFile = File(responsesPath)
                if (responseFile.exists()) {
                    call.respondFile(responseFile)
                } else {
                    call.respond(HttpStatusCode.NotFound, "Response file not found.")
                }
            }
        }
    }.start(wait = true)
}
