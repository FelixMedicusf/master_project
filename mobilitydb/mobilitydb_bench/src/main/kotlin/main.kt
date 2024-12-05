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

fun main() {

    val user = "felix"
    val password = "master"
    val database = "aviation_data"

    val path = Paths.get("benchConf.yaml")

    val seed = 123L
    val random = Random(seed)

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


    // read and parse the yaml file and deserialize it into the data classes
    val config: BenchmarkConfiguration = Files.newBufferedReader(path).use { bufferedReader ->
        mapper.readValue(bufferedReader, BenchmarkConfiguration::class.java)
    }

    val threadCount: Int = config.benchmarkSettings.threads
    val sut: String = config.benchmarkSettings.sut
    val nodes: List<String> = config.benchmarkSettings.nodes

    val allQueries: MutableList<QueryTask> = mutableListOf()


    for(queryConfig in config.queryConfigs){
        if (queryConfig.use){
            if (queryConfig.parameters != null){
                repeat(queryConfig.repetition) {
                    allQueries.add(QueryTask(queryConfig.name, queryConfig.type, queryConfig.sql, queryConfig.parameters))
                }
            } else {
                repeat(queryConfig.repetition) {
                    allQueries.add(QueryTask(queryConfig.name, queryConfig.type, queryConfig.sql))
                }
            }
        }
    }

    allQueries.shuffle(random)

    val startLatch = CountDownLatch(1)
    val executionLogs = Collections.synchronizedList(mutableListOf<QueryExecutionLog>())
    val benchThreads = Executors.newFixedThreadPool(threadCount)

    /*
    val threadsQueryList: MutableList<MutableList<QueryTask>> =
        MutableList(threadCount) { mutableListOf() }

    distributeQueryTasks(allQueries, threadsQueryList)

    for ((threadNumber, queries) in threadsQueryList.withIndex()){
        benchThreads.submit(BenchThread("thread-$threadNumber", nodes[0], database, user,
            password, queries, executionLogs, startLatch))
    }

     */

    val queryQueue = ConcurrentLinkedQueue(allQueries)

    for (i in 1..threadCount){
        benchThreads.submit(BenchThread("thread-$i", nodes[0], database, user, password, queryQueue, executionLogs, startLatch))
    }

    // Signal all threads to start
    println("Releasing $threadCount threads to start execution.")
    startLatch.countDown()


    benchThreads.shutdown()
    benchThreads.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)

    File(benchmarkLogsPath).writeText(executionLogs.joinToString(separator = "\n"))

    println("Execution logs have been written to $benchmarkLogsPath")


}

fun distributeQueryTasks(sourceList: MutableList<QueryTask>, targetList: MutableList<MutableList<QueryTask>>) {
    for ((index, element) in sourceList.withIndex()) {
        val targetIndex = index % targetList.size
        targetList[targetIndex].add(element)
    }
}

fun castToNaturalType(input: String): Any {
    return when {
        input.equals("true", ignoreCase = true) -> true // Check for boolean true
        input.equals("false", ignoreCase = true) -> false // Check for boolean false
        input.toIntOrNull() != null -> input.toInt() // Check if it's an integer
        input.toDoubleOrNull() != null -> input.toDouble() // Check if it's a floating-point number
        else -> input // Default to the original string
    }
}

