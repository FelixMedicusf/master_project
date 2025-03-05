import com.fasterxml.jackson.databind.SerializationFeature
import io.ktor.serialization.jackson.*
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.runBlocking
import java.io.File

class BenchmarkClient(private val serverUrl: String) {

    private val client = HttpClient(CIO) {
        install(HttpTimeout) {
            requestTimeoutMillis = 12_000_000 // 100 minutes
            connectTimeoutMillis = 10_000  // 10 seconds
            socketTimeoutMillis = 12_000_000  // 100 minutes
        }
        install(ContentNegotiation) {
            jackson {
                enable(SerializationFeature.INDENT_OUTPUT) // Enable pretty-print
            }
        }
    }

    fun uploadConfig(filePath: String) = runBlocking {
        try {
            val configFile = File(filePath)
            if (!configFile.exists()) {
                println("Configuration file does not exist: $filePath")
                return@runBlocking
            }

            val response: HttpResponse = client.post("$serverUrl/upload-config") {
                setBody(configFile.readBytes())
                contentType(ContentType.Application.OctetStream)
            }

            println("Response: ${response.status}")
            println("Message: ${response.bodyAsText()}")
        } catch (e: Exception) {
            println("Error uploading configuration: ${e.message}")
        }
    }

    fun startBenchmark() = runBlocking {
        try {
            val response: HttpResponse = client.post("$serverUrl/start-benchmark")
            println("Response: ${response.status}")
            println("Message: ${response.bodyAsText()}")

        } catch (e: Exception) {
            println("Error starting benchmark: ${e.message}")
        }
    }

    fun retrieveLogs(destinationPath: String) = runBlocking {
        try {
            val response: HttpResponse = client.get("$serverUrl/retrieve-logs")
            if (response.status == HttpStatusCode.OK) {
                val logFile = File(destinationPath)
                logFile.writeBytes(response.readBytes())
                println("Logs successfully saved to $destinationPath")
            } else {
                println("Error retrieving logs: ${response.status}")
                println("Message: ${response.bodyAsText()}")
            }
        } catch (e: Exception) {
            println("Error retrieving logs: ${e.message}")
        }
    }

    fun retrieveResponses(destinationPath: String) = runBlocking {
        try {
            val response: HttpResponse = client.get("$serverUrl/retrieve-responses")
            if (response.status == HttpStatusCode.OK) {
                val logFile = File(destinationPath)
                logFile.writeBytes(response.readBytes())
                println("Query responses successfully saved to $destinationPath")
            } else {
                println("Error retrieving responses: ${response.status}")
                println("Message: ${response.bodyAsText()}")
            }
        } catch (e: Exception) {
            println("Error retrieving responses: ${e.message}")
        }
    }

    fun triggerDataHandler(executionPattern: List<Int>) = runBlocking {
        try {
            val response: HttpResponse = client.post("$serverUrl/data-handler") {
                contentType(ContentType.Application.Json)
                setBody(executionPattern)
            }
            if (response.status == HttpStatusCode.Accepted) {
                println("DataHandler process has started on the server.")
            } else {
                println("Failed to start DataHandler: ${response.status}")
                println("Message: ${response.bodyAsText()}")
            }
        } catch (e: Exception) {
            println("Error triggering DataHandler: ${e.message}")
        }
    }
}

fun main() {

    val distributed = false
    val mongodb = false
    val loadPhase = false
    val runPhase = false
    val benchmarkConducted = true
    val test = false

    val configPathMongoDBSingle = "benchConfigMongoDBSingle.yaml"
    val configPathMobilityDBSingle = "benchConfigMobilityDBSingle.yaml"

    val configPathMongoDBCluster = "benchConfigMongoDBCluster.yaml"
    val configPathMobilityDBCluster = "benchConfigMobilityDBCluster.yaml"

    val benchmarkingClientHost = "35.205.243.199"
    val databaseClientAddress = "$benchmarkingClientHost:8080"

    var path = ""
    path = if (mongodb) {
        if (!distributed) configPathMongoDBSingle else configPathMongoDBCluster
    } else {
        if (!distributed) configPathMobilityDBSingle else configPathMobilityDBCluster
    }



    val serverUrl = "http://$databaseClientAddress"
    val client = BenchmarkClient(serverUrl)

    println("\n1. Uploading configuration $path")
    client.uploadConfig(path)

    /*
    for MongoDB (execution pattern):
    handler.updateDatabaseCollections() --> 0 index
    handler.insertRegionalData() --> 1 index
    handler.shardCollections() --> 2 index
    handler.createFlightTrips() --> 3 index
    handler.createTrajectories(separators) --> 4 index
    handler.flightPointsMigration(separators) --> 5 index
    handler.flightPointsInterpolation(separators) --> 6 index
    handler.createTimeSeriesCollectionIndexes() -- 7 index

     */

    // must be of size 8
    val executionPattern = listOf(0, 0, 0, 0, 1, 1, 1, 1)

    if (mongodb && loadPhase) {
        println("\n2. Triggering DataHandler operations for MongoDB...")
        client.triggerDataHandler(executionPattern)
    }


    if(runPhase){
        client.startBenchmark()
    }

//    println("\n4. Stopping benchmark...")
//    client.stopBenchmark()

    if (benchmarkConducted){
        println("\n5. Retrieving logs...")
        client.retrieveLogs("src/main/resources/benchmark_execution_logs.txt") // Replace with the actual destination path
        if(test){
            client.retrieveResponses("src/test/resources/query_responses.txt")

        }
    }

}






