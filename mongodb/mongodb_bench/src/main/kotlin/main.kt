import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.KotlinFeature
import com.fasterxml.jackson.module.kotlin.KotlinModule
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.random.Random

const val USER = "felix"
const val PASSWORD = "master"
const val DATABASE = "aviation_data"


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

    fun execute() {
        val config = loadConfig() ?: return

        val threadCount = config.benchmarkSettings.threads
        val nodes = config.benchmarkSettings.nodes
        val mainSeed = config.benchmarkSettings.randomSeed ?: 123L
        println("Using random seed: $mainSeed")

        val allQueries = prepareQueryTasks(config)
        allQueries.shuffle(Random(mainSeed))

        val executionLogs = Collections.synchronizedList(mutableListOf<QueryExecutionLog>())
        val threadSeeds = mutableListOf<Long>()
        val queryQueue = ConcurrentLinkedQueue(allQueries)
        val startLatch = CountDownLatch(1)

        val benchThreads = Executors.newFixedThreadPool(threadCount)
        for (i in 1..threadCount) {
            val threadSeed = generateRandomSeed(mainSeed)
            threadSeeds.add(threadSeed)
            benchThreads.submit(
                BenchThread(
                    "thread-$i", nodes, queryQueue, executionLogs, startLatch, threadSeed
                )
            )
        }

        println("Releasing $threadCount threads to start execution.")
        startLatch.countDown()

        benchThreads.shutdown()
        benchThreads.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)

        saveExecutionLogs(threadSeeds, executionLogs)
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

    private fun prepareQueryTasks(config: BenchmarkConfiguration): MutableList<QueryTask> {
        val allQueries = mutableListOf<QueryTask>()
        for (queryConfig in config.queryConfigs) {
            if (queryConfig.use) {
                val repetitions = List(queryConfig.repetition) {
                    QueryTask(queryConfig.name, queryConfig.type, queryConfig.mongodbQuery, queryConfig.parameters)
                }
                allQueries.addAll(repetitions)
            }
        }
        return allQueries
    }

    private fun generateRandomSeed(existingSeed: Long): Long {
        return Random(existingSeed).nextLong()
    }

    private fun saveExecutionLogs(threadSeeds: List<Long>, executionLogs: List<QueryExecutionLog>) {
        File(logsPath).writeText(threadSeeds.joinToString(separator = ";") + "\n")
        File(logsPath).appendText(executionLogs.joinToString(separator = "\n"))
        println("Execution logs have been written to $logsPath")
    }
}

fun main() {
    val executor = BenchmarkExecutor("benchConf.yaml", "src/main/resources/benchmark_execution_logs.txt")
    executor.execute()
}


/*
fun main() {



    val path = Paths.get("benchConf.yaml")


    val benchmarkLogsPath = "src/main/resources/benchmark_execution_logs.txt"

    val mapper = ObjectMapper(YAMLFactory()).apply {
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


    val config: BenchmarkConfiguration = try {
        Files.newBufferedReader(path).use { bufferedReader ->
            mapper.readValue(bufferedReader, BenchmarkConfiguration::class.java)
        }
    } catch (e: Exception) {
        println("Error reading or parsing configuration file: ${e.message}")
        return
    }

    val threadCount: Int = config.benchmarkSettings.threads
    val sut: String = config.benchmarkSettings.sut
    val nodes: List<String> = config.benchmarkSettings.nodes

    val mainSeed = config.benchmarkSettings.randomSeed ?: 123L
    println("Using random seed: $mainSeed")
    val random = Random(mainSeed)
    val allQueries: MutableList<QueryTask> = mutableListOf()


    for(queryConfig in config.queryConfigs){
        if (queryConfig.use){
            if (queryConfig.parameters != null){
                repeat(queryConfig.repetition) {
                    allQueries.add(QueryTask(queryConfig.name, queryConfig.type, queryConfig.mongoQuery, queryConfig.parameters))
                }
            } else {
                repeat(queryConfig.repetition) {
                    allQueries.add(QueryTask(queryConfig.name, queryConfig.type, queryConfig.mongoQuery))
                }
            }
        }
    }

    allQueries.shuffle(random)

    val threadSeeds = ArrayList<Long>()

    val startLatch = CountDownLatch(1)
    val executionLogs = Collections.synchronizedList(mutableListOf<QueryExecutionLog>())
    val benchThreads = Executors.newFixedThreadPool(threadCount)

    val queryQueue = ConcurrentLinkedQueue(allQueries)

    for (i in 1..threadCount){
        val threadSeed = generateRandomSeed(mainSeed)
        threadSeeds.add(threadSeed)
        benchThreads.submit(BenchThread("thread-$i", nodes, DATABASE, USER, PASSWORD, queryQueue, executionLogs, startLatch, threadSeed))
    }

    // Signal all threads to start
    println("Releasing $threadCount threads to start execution.")
    startLatch.countDown()


    benchThreads.shutdown()
    benchThreads.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)

    File(benchmarkLogsPath).writeText(threadSeeds.joinToString(separator = ";") + "\n")
    File(benchmarkLogsPath).appendText(executionLogs.joinToString(separator = "\n"))

    println("Execution logs have been written to $benchmarkLogsPath")

}

fun generateRandomSeed(existingSeed: Long): Long {
    val random = Random(existingSeed) // Initialize Random with the existing seed
    return random.nextLong()          // Generate a random Long
}

fun distributeQueryTasks(sourceList: MutableList<QueryTask>, targetList: MutableList<MutableList<QueryTask>>) {
    for ((index, element) in sourceList.withIndex()) {
        val targetIndex = index % targetList.size
        targetList[targetIndex].add(element)
    }
}

 */


