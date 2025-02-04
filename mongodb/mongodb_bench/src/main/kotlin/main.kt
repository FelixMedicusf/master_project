import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.KotlinFeature
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.mongodb.MongoClientSettings
import com.mongodb.MongoCredential
import com.mongodb.ServerAddress
import com.mongodb.client.MongoClients
import com.mongodb.connection.ClusterSettings
import io.ktor.http.*
import io.ktor.serialization.jackson.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.plugins.statuspages.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import org.bson.Document
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.concurrent.*
import kotlin.random.Random


val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
const val USER = "felix"
const val PASSWORD = "master"
const val DATABASE = "aviation_data"
var benchmarkExecutorService: ExecutorService? = null
val separators = listOf(0, 681642631, 701642631, 710076001, 718926541, 728177911, 736845861, 745346091, 755447851, 765304441, 772385481, 774441640)

class BenchmarkExecutor(private val configPath: String, private val logsPath: String) {

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

    private val seeds = mutableListOf<Long>()

    fun execute() {
        val config = loadConfig() ?: return

        val threadCount = config.benchmarkSettings.threads
        val nodes = config.benchmarkSettings.nodes
        val mainSeed = config.benchmarkSettings.randomSeed
        val sut = config.benchmarkSettings.sut
        val mainRandom = Random(mainSeed)
        val warmUpRandom = Random(4545)
        println("Using random seed: $mainSeed")

        val allQueries = prepareQueryTasks(config, mainRandom)
        val executionLogs = Collections.synchronizedList(mutableListOf<QueryExecutionLog>())
        val threadSafeQueries = ConcurrentLinkedQueue(allQueries)
        val startLatch = CountDownLatch(1)

        warmUpSut(nodes, 50, warmUpRandom)

        val benchThreads = Executors.newFixedThreadPool(threadCount)
        val threadSeeds = generateRandomSeeds(mainRandom, threadCount)
        for (i in 0..<threadCount) {
            benchThreads.submit(
                BenchThread(
                    "thread-$i", nodes, threadSafeQueries, executionLogs, startLatch, threadSeeds[i]
                )
            )
        }

        println("Releasing $threadCount threads to start execution.")
        val benchStart = Instant.now().toEpochMilli()
        startLatch.countDown()

        benchThreads.shutdown()
        benchThreads.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)
        val benchEnd = Instant.now().toEpochMilli()

        mergeLogFilesAndCleanUp("mongo_response_log_combined.txt", "mongo_response_log_.*\\.txt")
        saveExecutionLogs(threadSeeds, executionLogs, benchStart, benchEnd, nodes.size, sut, mainSeed)
        benchmarkExecutorService!!.shutdownNow()

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
                    val paramValues = returnParamValues(queryConfig.parameters, random)
                    allQueries.add(QueryTask(queryConfig.name, queryConfig.type, paramValues))
                }
            }
        }
        allQueries.shuffle(random)
        return allQueries
    }

    private fun generateRandomSeeds(mainRandom: Random, threadCount: Int): List<Long> {
        val seeds = mutableListOf<Long>()
        for (i in 0..<threadCount) {
            seeds.add(mainRandom.nextLong())
        }
        return seeds
    }

    private fun returnParamValues(params: List<String>, random: Random): List<Any> {
        var values = ArrayList<Any>()
        for (param in params){
            val replacement = when (param) {
                "period_short" -> generateRandomTimeSpan(random=random, formatter = formatter, year=2023, mode=1)
                "period_medium" -> generateRandomTimeSpan(random=random, formatter = formatter, year=2023, mode=2)
                "period_long" -> generateRandomTimeSpan(random=random, formatter = formatter, year=2023, mode=3)
                "period" -> generateRandomTimeSpan(formatter = formatter, year=2023, random = random)
                "instant" -> generateRandomTimestamp(formatter = formatter, random = random)
                "day" -> getRandomDay(random = random, year = 2023)
                "city" -> getRandomPlace(cities, "name", random)
                "municipality" -> getRandomPlace(municipalities, "name", random)
                "county" -> getRandomPlace(counties, "name", random)
                "district" -> getRandomPlace(districts, "name", random)
                "point" -> getRandomPoint(random, listOf(listOf(6.212909, 52.241256), listOf(8.752841, 50.53438)))
                "radius" -> (random.nextDouble(0.25, 0.5) * 10)/6378.1
                "low_altitude" -> (random.nextInt(5, 60) * 10)
                "distance" -> (random.nextInt(5, 30) * 10)
                else -> ""

            }
            if (replacement != null) {
                values.add(replacement)
            }

        }
        return values
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

    private fun saveExecutionLogs(threadSeeds: List<Long>, executionLogs: List<QueryExecutionLog>, benchStart: Long, benchEnd: Long, nodeNumber: Int, sut: String, mainSeed: Long) {

        val file = File(logsPath)
        file.writeText("start: ${Date(benchStart)}, end: ${Date(benchEnd)}, duration (s): ${(benchEnd - benchStart)/1000}. " + "SUT: $sut, " + "#threads: ${threadSeeds.size}, " + "#nodes: $nodeNumber, queries executed: ${executionLogs.size}. Seed: $mainSeed" + "\n")
        file.appendText("threadName, queryName, queryType, parameter, parameterValues, round, executionIndex, startFirstQuery, endFirstQuery, startSecQuery, endSecQuery, latency, fetchedRecords\n")
        file.appendText(executionLogs.joinToString(separator = "\n"))
        println("Execution logs have been written to $logsPath")
    }

    private fun warmUpSut(mongodbIps: List<String>, repetitions: Int = 50, warmUpRandom: Random){

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
                        USER,
                        "admin",
                        PASSWORD.toCharArray()
                    )
                )
                .build())

        val mongoDatabase = conn.getDatabase(DATABASE)

        val citiesCollection = mongoDatabase.getCollection("cities")
        val municipalitiesCollection = mongoDatabase.getCollection("municipalities")
        val countiesCollection = mongoDatabase.getCollection("counties")
        val districtsCollection = mongoDatabase.getCollection("districts")
        val flightPointsCollection = mongoDatabase.getCollection("flightpoints")
        val flightPointsTsCollection = mongoDatabase.getCollection("flightpoints_ts")
        val flightTripsCollection = mongoDatabase.getCollection("flighttrips")
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
                val randomTimespan = generateRandomTimeSpan(warmUpRandom, formatter, 2023, 1)

                flightPointsTsCollection.aggregate(listOf(Document("\$match", Document("flightId", randomFlightId))))
                flightTripsCollection.aggregate(listOf(Document("\$match", Document("flightId", randomFlightId))))
                flightPointsCollection.aggregate(listOf(Document("\$match", Document("flightId", randomFlightId))))
                countiesCollection.aggregate(listOf(Document("\$match", Document("name", randomCounty))))
                municipalitiesCollection.aggregate(listOf(Document("\$match", Document("name", randomMunicipality))))
                districtsCollection.aggregate(listOf(Document("\$match", Document("name", randomDistrict))))
                citiesCollection.aggregate(listOf(Document("\$match", Document("name", randomCity))))

                flightPointsTsCollection.aggregate(listOf(
                    Document(
                        "\$match",
                            Document(
                                "timestamp", Document("\$gte", randomTimespan[0]).append("\$lte", randomTimespan[1]))),
                    Document(
                        "\$group", Document()
                            .append(
                                "_id", Document()
                                    .append("flightId", "\$metadata.flightId").append("track","\$metadata.track")))))


                val firstPipeline = listOf(
                    Document("\$match", Document("name", randomMunicipality)),
                    Document("\$project", Document("polygon.coordinates", 1).append("_id", 0).append("name", 1))
                )
                val firstResponse = municipalitiesCollection.aggregate(firstPipeline).toList()

                val polygonCoordinates = firstResponse.mapNotNull { document ->
                    val polygon = document.get("polygon", Document::class.java) // Get 'polygon' sub-document
                    polygon?.get("coordinates", List::class.java) // Get 'coordinates' as a List
                }[0]

                flightPointsTsCollection.aggregate(listOf(
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
                    Document("\$limit", 5)),

                )


                i++
            }

        } catch (e: Exception) {
            e.printStackTrace()
        } finally {
            println("Finished warm up phase of SUT.")
            conn.close()
        }
    }

    private fun mergeLogFilesAndCleanUp(outputFileName: String, logFilePattern: String) {
        val outputFile = File(outputFileName)

        if (outputFile.exists()) {
            outputFile.delete()
        }

        val logFiles = File(".").listFiles { _, name ->
            name.matches(Regex(logFilePattern))
        } ?: emptyArray()

        outputFile.bufferedWriter().use { writer ->
            for (logFile in logFiles) {
                writer.appendLine("=== Content from ${logFile.name} ===")
                logFile.forEachLine { line ->
                    writer.appendLine(line)
                }
                writer.appendLine()
            }
        }

        logFiles.forEach { it.delete() }
        println("Merged ${logFiles.size} log files into ${outputFile.absolutePath}. Deleted thread-specific log files.")
    }

    private fun generateRandomTimeSpan(random: Random, formatter: DateTimeFormatter, year: Int, mode: Int = 0): List<String> {

        // Generate pseudo random timestamps based on the mode
        // 1: for short time range (0-2   days), 2: for medium time range (2 days - 1 month), 3: for long time range (1 - 12 Month)
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

    private fun getRandomPlace(
        parsedData: List<Map<String, String>>,
        columnName: String,
        random: Random
    ): String? {

        val columnValues = parsedData.mapNotNull { it[columnName] }

        if (columnValues.isEmpty()) return null

        return columnValues[random.nextInt(columnValues.size)]
    }

}

fun main() {
    // Path to the config and logs
    val configPath = "benchConf.yaml"
    val logsPath = "mongo_benchmark_execution_logs.txt"

    // Start HTTP server
    embeddedServer(Netty, port = 8080) {
        install(ContentNegotiation) {
            jackson {
                enable(SerializationFeature.INDENT_OUTPUT) // Pretty print JSON responses
            }
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

            println("Starting Benchmarking Server v1.3")
            // Maximum memory the JVM can use (heap size limit)
            val runtime = Runtime.getRuntime()
            val maxMemory = runtime.maxMemory() / (1024 * 1024)
            println("Max Memory (heap limit): ${maxMemory} MB")



            post("/create-trajectories") {
                try {
                    val handler = DataHandler(DATABASE)

                    try {
                        handler.createTrajectories(separators)
                        println("Trajectories created.")
                        call.respond(HttpStatusCode.OK, "Created trajectories.")
                    } catch (e: Exception) {
                        e.printStackTrace()
                        println("Error during creation of trajectories: ${e.message}")
                        call.respond(
                            HttpStatusCode.InternalServerError,
                            "Error during trajectories creation: ${e.message}"
                        )
                    }
                } catch (e: Exception) {
                    e.printStackTrace()
                    call.respond(
                        HttpStatusCode.BadRequest,
                        "Invalid input for create-trajectories: ${e.message}"
                    )
                }
            }

            post("/migrate-flightpoints") {
                try {
                    val handler = DataHandler(DATABASE)

                    try {
                        handler.flightPointsMigration(separators)
                        println("Migrated flightpoints to time series collection.")
                        call.respond(HttpStatusCode.OK, "Migrated Flightpoints.")
                    } catch (e: Exception) {
                        e.printStackTrace()
                        println("Error during creation of time series data: ${e.message}")
                        call.respond(
                            HttpStatusCode.InternalServerError,
                            "Error during migration of flightpoints to time series collection: ${e.message}"
                        )
                    }
                } catch (e: Exception) {
                    e.printStackTrace()
                    call.respond(
                        HttpStatusCode.BadRequest,
                        "Invalid input for migrate-flightpoints: ${e.message}"
                    )
                }
            }

            post("/interpolate-flightpoints") {
                try {
                    val handler = DataHandler(DATABASE)

                    try {
                        handler.flightPointsInterpolation(separators)
                        println("Interpolated flight points in time series collection.")
                        call.respond(HttpStatusCode.OK, "Interpolated flightpoints.")
                    } catch (e: Exception) {
                        e.printStackTrace()
                        println("Error during interpolation of flightpoints: ${e.message}")
                        call.respond(
                            HttpStatusCode.InternalServerError,
                            "Error during interpolation of flightpoints: ${e.message}"
                        )
                    }
                } catch (e: Exception) {
                    e.printStackTrace()
                    call.respond(
                        HttpStatusCode.BadRequest,
                        "Invalid input for interpolate-flightpoints: ${e.message}"
                    )
                }
            }


            post("/data-handler") {
                try {
                    val handler = DataHandler(DATABASE)

                    var executionPattern = call.receive<List<Int>>()
                    if(executionPattern.isEmpty())executionPattern= listOf(1, 1, 1, 1, 1, 1, 1, 1)

                    try {

                        if(executionPattern[0] == 1){
                            handler.updateDatabaseCollections()
                        }
                        if(executionPattern[1] == 1){
                            handler.insertRegionalData()
                        }
                        if(executionPattern[2] == 1){
                            handler.shardCollections()
                        }
                        if(executionPattern[3] == 1){
                            handler.createFlightTrips()
                        }
                        if(executionPattern[4] == 1){
                            handler.createTrajectories(separators)
                        }
                        if(executionPattern[5] == 1){
                            handler.flightPointsMigration(separators)
                        }
                        if(executionPattern[6] == 1){
                            handler.flightPointsInterpolation(separators)
                        }
                        if(executionPattern[7] == 1){
                            handler.createTimeSeriesCollectionIndexes()
                        }

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


            post("/upload-config") {
                println("Uploading benchmark configurations.")
                val configFileBytes = call.receive<ByteArray>()
                File(configPath).writeBytes(configFileBytes)
                call.respond(HttpStatusCode.OK, "Configuration file uploaded.")
            }

            // Retrieve benchmark logs
            get("/retrieve-logs") {
                println("Received request for the retrieval of the benchmark logs.")
                val logFile = File(logsPath)
                if (logFile.exists()) {
                    call.respondFile(logFile)
                } else {
                    call.respond(HttpStatusCode.NotFound, "Log file not found.")
                }
            }
        }
    }.start(wait = true)
}


fun parseCSV(filePath: String, requiredColumns: Set<String>): List<Map<String, String>> {
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