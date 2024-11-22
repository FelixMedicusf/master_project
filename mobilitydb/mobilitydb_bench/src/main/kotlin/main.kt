import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.KotlinFeature
import com.fasterxml.jackson.module.kotlin.KotlinModule
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
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

    val threadsQueryList: MutableList<MutableList<QueryTask>> =
        MutableList(threadCount) { mutableListOf() }

    distributeQueryTasks(allQueries, threadsQueryList)

    for ((threadNumber, queries) in threadsQueryList.withIndex()){
        benchThreads.submit(BenchThread("thread-$threadNumber", nodes[0], database, user,
            password, queries, executionLogs, startLatch))
    }

    // Signal all threads to start
    println("Releasing threads to start execution.")
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

/*

    // Group tasks by rounds (parameter sets)
    val tasksByRound = mutableListOf<List<QueryTask>>()
    val maxRounds = config.queries.maxOf { it.paramSets.size }
    repeat(maxRounds) { roundIndex ->
        val roundTasks = config.queries.flatMap { query ->
            query.paramSets.getOrNull(roundIndex)?.let { paramSet ->
                QueryTask(
                    queryName = query.name,
                    sql = query.sql,
                    params = paramSet.params,
                    executions = paramSet.executions
                )
            } ?: emptyList()
        }
        tasksByRound.add(roundTasks)
    }

    // Distribute tasks by rounds across threads
    val threadTasksByRound = List(threadCount) { mutableListOf<List<QueryTask>>() }
    tasksByRound.forEachIndexed { roundIndex, roundTasks ->
        val threadChunkedTasks = roundTasks.chunked((roundTasks.size + threadCount - 1) / threadCount)
        threadChunkedTasks.forEachIndexed { threadIndex, tasks ->
            threadTasksByRound[threadIndex].add(tasks)
        }
    }

    // Latch to ensure threads start simultaneously
    val startLatch = CountDownLatch(1)

    // Create a fixed thread pool
    val benchThreads = Executors.newFixedThreadPool(threadCount)

    // Submit threads to the pool
    threadTasksByRound.forEachIndexed { index, tasksByThreadRound ->
        benchThreads.submit(
            BenchThread(
                threadName = "Thread-${index + 1}",
                mobilityDBIp = "127.0.0.1",
                databaseName = "mobilitydb",
                user = "postgres",
                password = "password",
                tasksByRound = tasksByThreadRound,
                log = executionLogs,
                startLatch = startLatch
            )
        )
    }

    // Signal all threads to start
    println("Releasing threads to start execution.")
    startLatch.countDown()

    // Wait for all threads to complete
    benchThreads.shutdown()
    benchThreads.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)

    println("All threads completed.")

    // Output logs
    outputExecutionLogs(executionLogs)
}

*/
