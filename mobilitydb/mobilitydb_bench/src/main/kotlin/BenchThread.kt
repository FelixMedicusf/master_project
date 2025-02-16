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
    private val logResponses: Boolean
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
        var printStream: PrintStream? = null


        if(logResponses){
            val logFile = File("sql_response_log_${threadName}.txt")
            if (logFile.exists()) {
                logFile.delete()
            }

            logFile.createNewFile()
            printStream = PrintStream(logFile)
        }

        try {

            val connectionString = "jdbc:postgresql://$mobilityDBIp:5432/$DATABASE?ApplicationName=bench-$threadName&autoReconnect=true"
            connection = getConnection(
                connectionString, USER, PASSWORD
            )

            statement = connection.createStatement()

            if (threadName == "thread-0"){
                statement.executeUpdate("SET citus.max_intermediate_result_size TO '12GB';")
            }

            // Ensure all threads start at the same time
            startLatch.await()

            if(logResponses){
                printStream?.println("$threadName started executing at ${Instant.now()}.")
            }

            println()
            var i = 1
            println("$threadName started executing at ${Instant.now()}.")

            while (true) {
                val task = queryQueue.poll() ?: break


                if(logResponses) {
                    printStream?.println("$i: ${task.queryName} with params: ${task.paramValues}. ${task.parsedSql}")
                }



                val startTime = Instant.now().toEpochMilli()
                val response = statement.executeQuery(task.parsedSql)
                val endTime = Instant.now().toEpochMilli()


                val parameterValues = task.paramValues.joinToString(";") ?: ""


                if(logResponses) {
                    response.use { resultSet ->
                        // Get metadata to print column headers
                        val metaData = resultSet.metaData
                        val columnCount = metaData.columnCount

                        // Print column headers
                        for (j in 1..columnCount) {
                            printStream?.print("${metaData.getColumnName(i)}\t") // Tab-separated for better readability
                        }
                        printStream?.println() // Move to the next line after printing column headers

                        // Print all rows
                        while (resultSet.next()) {
                            for (j in 1..columnCount) {
                                val value = resultSet.getString(i) ?: "NULL" // Handle NULL values
                                printStream?.print("$value\t")
                            }
                            printStream?.println() // Move to the next line after each row
                        }

                        printStream?.println() // Add an extra line for separation after the response
                    }
                }


                synchronized(log) {
                    log.add(
                        QueryExecutionLog(
                            threadName = threadName,
                            queryName = task.queryName,
                            queryType = task.type,
                            paramValues = parameterValues.replace(",", "/"),
                            startTimeFirst = startTime,
                            endTimeFirst = endTime,
                            startTimeSecond = 0,
                            endTimeSecond = 0,
                            latency = (endTime - startTime),
                        )
                    )
                }

                i++

                if(i%50==0){
                    println("$threadName processed $i queries.")
                }
            }

            if(logResponses){
                printStream?.println("$threadName finished executing at ${Instant.now()}.")
            }

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



}





