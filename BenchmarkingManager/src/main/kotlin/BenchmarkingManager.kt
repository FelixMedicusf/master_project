import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.math.ceil
import kotlin.math.floor

class BenchmarkClient(private val serverUrl: String) {

    private val client = HttpClient(CIO)

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

    /**
     * Start the benchmark execution
     */
    fun startBenchmark() = runBlocking {
        try {
            val response: HttpResponse = client.post("$serverUrl/start-benchmark")
            println("Response: ${response.status}")
            println("Message: ${response.bodyAsText()}")
        } catch (e: Exception) {
            println("Error starting benchmark: ${e.message}")
        }
    }

    fun stopBenchmark() = runBlocking {
        try {
            val response: HttpResponse = client.post("$serverUrl/stop-benchmark")
            println("Response: ${response.status}")
            println("Message: ${response.bodyAsText()}")
        } catch (e: Exception) {
            println("Error stopping benchmark: ${e.message}")
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

    fun triggerDataHandler() = runBlocking {
        try {
            val response: HttpResponse = client.post("$serverUrl/data-handler")
            if (response.status == HttpStatusCode.OK) {
                println("DataHandler operations completed successfully.")
            } else {
                println("Error triggering DataHandler: ${response.status}")
                println("Message: ${response.bodyAsText()}")
            }
        } catch (e: Exception) {
            println("Error triggering DataHandler: ${e.message}")
        }
    }
}

fun main() {

//    val configPathMongoDB = "benchConfigMongoDB.yaml"
//    val configPathMobilityDB = "benchConfigMobilityDB.yaml"
//
//    val databaseClientAddress = "34.140.86.175:8080"
//
//    val serverUrl = "http://$databaseClientAddress"
//    val client = BenchmarkClient(serverUrl)
//
//    println("\n1. Uploading configuration...")
//    client.uploadConfig(configPathMongoDB)
//
//    println("\n2. Triggering DataHandler operations...")
//    client.triggerDataHandler()
//
//
//    println("\n3. Starting benchmark...")
//    client.startBenchmark()

//    println("\n4. Stopping benchmark...")
//    client.stopBenchmark()

//    println("\n5. Retrieving logs...")
//    client.retrieveLogs("src/main/resources/benchmark_execution_logs.txt") // Replace with the actual destination path


}






