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
import java.time.Instant


const val DATABASE = "aviation_data"
const val USER = "felix"
const val PASSWORD = "master"
var benchmarkExecutorService: ExecutorService? = null



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
        val mainRandom = Random(mainSeed)
        println("Using random seed: $mainSeed")

        val allQueries = prepareQueryTasks(config, mainSeed)
        val executionLogs = Collections.synchronizedList(mutableListOf<QueryExecutionLog>())
        val queryQueue = ConcurrentLinkedQueue(allQueries)
        val startLatch = CountDownLatch(1)

        val benchThreads = Executors.newFixedThreadPool(threadCount)
        val threadSeeds = generateRandomSeeds(mainRandom, threadCount)
        for (i in 0 until threadCount) {

            benchThreads.submit(
                BenchThread(
                    "thread-$i", nodes[0], queryQueue, executionLogs, startLatch, threadSeeds[i]
                )
            )
        }

        println("Releasing $threadCount threads to start execution.")
        val benchStart = Instant.now().toEpochMilli()
        startLatch.countDown()

        benchThreads.shutdown()
        benchThreads.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)
        val benchEnd = Instant.now().toEpochMilli()

        saveExecutionLogs(threadSeeds, executionLogs, benchStart, benchEnd, nodes.size, sut)
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

    private fun prepareQueryTasks(config: BenchmarkConfiguration, seed: Long): MutableList<QueryTask> {
        val random = Random(seed)
        val allQueries = mutableListOf<QueryTask>()
        for (queryConfig in config.queryConfigs) {
            if (queryConfig.use) {
                if (queryConfig.parameters != null) {
                    repeat(queryConfig.repetition) {
                        allQueries.add(
                            QueryTask(queryConfig.name, queryConfig.type, queryConfig.sql, queryConfig.parameters)
                        )
                    }
                } else {
                    repeat(queryConfig.repetition) {
                        allQueries.add(QueryTask(queryConfig.name, queryConfig.type, queryConfig.sql))
                    }
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

    private fun saveExecutionLogs(threadSeeds: List<Long>, executionLogs: List<QueryExecutionLog>, benchStart: Long, benchEnd: Long, nodeNumber: Int, sut: String) {

        val file = File(logsPath)
        file.appendText("threadName, queryName, queryType, parameter, parameterValues, round, executionIndex, startFirstQuery, endFirstQuery, startSecQuery, endSecQuery, latency, fetchedRecords")
        file.appendText("start: ${Date(benchStart)}, end :${Date(benchEnd)}, duration (s): ${(benchEnd - benchStart)/1000}. " + "SUT: $sut, " + "#threads: ${threadSeeds.size}, "+ "thread seeds:" + threadSeeds.joinToString(separator = ";") + ", #nodes: $nodeNumber, queries executed: ${executionLogs.size}." + "\n")
        file.appendText(executionLogs.joinToString(separator = "\n"))
        println("Execution logs have been written to $logsPath")
    }
}


fun main() {
    // Path to the config and logs
    val configPath = "benchConf.yaml"
    val logsPath = "src/main/resources/benchmark_execution_logs.txt"

    // Start HTTP server
    embeddedServer(Netty, port = 8080) {
        install(ContentNegotiation) {
            jackson {}
        }
        routing {
            post("/data-handler") {
                try {223451
                    val handler = DFSDataHandler(DATABASE)

                    // Perform DataHandler operations sequentially
                    handler.processFlightData()
                    handler.processStaticData()
                    handler.createIndexes()

                    call.respond(HttpStatusCode.OK, "DataHandler operations completed successfully.")
                } catch (e: Exception) {
                    e.printStackTrace()
                    call.respond(
                        HttpStatusCode.InternalServerError,
                        "Error during DataHandler operations: ${e.message}"
                    )
                }
            }


            // Start benchmark execution
            post("/start-benchmark") {
                if (benchmarkExecutorService != null && !benchmarkExecutorService!!.isShutdown) {
                    call.respond(HttpStatusCode.BadRequest, "Benchmark execution is already running.")
                    return@post
                }

                benchmarkExecutorService = Executors.newSingleThreadExecutor()
                benchmarkExecutorService!!.submit {
                    val executor = BenchmarkExecutor(configPath, logsPath)
                    executor.execute()
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
        }
    }.start(wait = true)
}
