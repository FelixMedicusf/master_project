import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.KotlinFeature
import com.fasterxml.jackson.module.kotlin.KotlinModule
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

fun main() {

    val user = "felix"
    val password = "master"
    val database = "aviation_data"

    val path = Paths.get("benchConf.yaml")

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
    }

    // read and parse the yaml file and deserialize it into the data classes
    val config: BenchmarkConfiguration = Files.newBufferedReader(path).use { bufferedReader ->
        mapper.readValue(bufferedReader, BenchmarkConfiguration::class.java)
    }

    val threadCount: Int = config.benchmarkSettings.threads
    val sut: String = config.benchmarkSettings.sut
    val nodes: List<String> = config.benchmarkSettings.nodes

    val allQueries: MutableList<QueryTask> = mutableListOf()


    for (queryConfig in config.queryConfigs){
        if (queryConfig.paramSets != null){
            for (params in queryConfig.paramSets) {
                for ((_, value) in params) {
                    allQueries.add(QueryTask(queryConfig.name, queryConfig.type, queryConfig.sql, value, queryConfig.use))
                }
            }
        }
        else {
            allQueries.add(QueryTask(queryConfig.name, queryConfig.type, queryConfig.sql, null, queryConfig.use))
        }
    }


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
    println("These are the logs:\n")
    executionLogs.forEach { log -> println(log) }


}

fun distributeQueryTasks(sourceList: MutableList<QueryTask>, targetList: MutableList<MutableList<QueryTask>>) {
    for ((index, element) in sourceList.withIndex()) {
        val targetIndex = index % targetList.size
        targetList[targetIndex].add(element)
    }
}

