import java.sql.Connection
import java.sql.DriverManager.getConnection
import java.sql.ResultSet
import java.sql.Statement
import java.time.Instant
import java.util.concurrent.CountDownLatch

class BenchThread(
    private val threadName: String,
    private val mobilityDBIp: String,
    private val databaseName: String,
    private val user: String,
    private val password: String,
    private val queries: List<QueryTask>,
    private val log: MutableList<QueryExecutionLog>,
    private val startLatch: CountDownLatch
) : Thread(threadName) {

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

            println("$threadName started executing at ${Instant.now()} with ${queries.size} queries.")
            println()

            for (task in queries) {
                if (task.use) {
                    val sqlQuery = if (task.params != null) {
                        this.formatSQLStatement(task.sql, task.params)
                    } else {
                        task.sql
                    }

                    val startTime = Instant.now().toEpochMilli()

                    printSQLResponse(statement.executeQuery(sqlQuery))

                    val endTime = Instant.now().toEpochMilli()

                    synchronized(log) {
                        log.add(
                            QueryExecutionLog(
                                threadName = threadName,
                                queryName = task.queryName,
                                queryType = task.type,
                                params = task.params,
                                round = 0,
                                executionIndex = 0,
                                startTime = startTime,
                                endTime = endTime,
                                latency = (endTime - startTime)/1000
                            )
                        )
                    }
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


    private fun formatSQLStatement(sql: String, params: Map<String, Any>): String {
        var parsedSql = sql
        for ((key, value) in params) {
            val replacement = when (value) {
                is String -> "'$value'"
                is Int, is Double -> value.toString()
                is Boolean -> if (value) "TRUE" else "FALSE"
                else -> throw IllegalArgumentException("Unsupported type for key: $key")
            }
            parsedSql = parsedSql.replace(":$key", replacement)
        }
        return parsedSql
    }

    fun printSQLResponse(resultSet: ResultSet) {
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

}
