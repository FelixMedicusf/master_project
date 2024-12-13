import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.KotlinFeature
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.mongodb.MongoClientSettings
import com.mongodb.MongoCredential
import com.mongodb.ServerAddress
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.connection.ClusterSettings
import org.bson.Document
import java.nio.file.Files
import java.nio.file.Paths
import kotlin.reflect.jvm.internal.impl.builtins.StandardNames


class DFSDataHandler {

    private val conf: BenchmarkConfiguration

    init {
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

        conf = Files.newBufferedReader(path).use { reader ->
            mapper.readValue(reader, BenchmarkConfiguration::class.java)
        }

        val mongodbIps = conf.benchmarkSettings.nodes
        val user = "felix"
        val password = "master"
        val connectionString = "mongodb://$user:$password@${mongodbIps.joinToString(",") { "$it:27017" }}/" +
                "<database>?replicaSet=<replicaSetName>&retryWrites=true&w=majority"


        val mongdbClientPort = 27017;

        var mongodbHosts = ArrayList<ServerAddress>();
        mongodbHosts.add(ServerAddress(mongodbIps[0], mongdbClientPort))
        mongodbHosts.add(ServerAddress(mongodbIps[1], mongdbClientPort))
        mongodbHosts.add(ServerAddress(mongodbIps[2], mongdbClientPort))

        val databaseName = "aviation_data"

        val conn = MongoClients.create(
            MongoClientSettings.builder()
                .applyToClusterSettings { builder: ClusterSettings.Builder ->
                    builder.hosts(
                        mongodbHosts
                    )
                }
                //.credential(MongoCredential.createCredential(user, databaseName, password.toCharArray()))
                .build())

        val database = conn.getDatabase(databaseName)


        fun createFlightPointsCollection(database: MongoDatabase){
            database.getCollection("flightpoints")
            val filePath = "flightpoints.csv"

        }

        val collection: MongoCollection<Document> = database.getCollection("flightPoints")

        collection.insertOne(Document("key", "value"))

        conn.close();

        /*
        val settings = MongoClientSettings.builder()
            .applyConnectionString(ConnectionString(connectionString))
            .build()

        val mongoClient = MongoClients.create(settings)

        val databaseName = "aviation_data"
        val database: MongoDatabase = mongoClient.getDatabase(databaseName)

        val collectionName = "flightPoints"
        val collection = database.getCollection(collectionName)
        val sampleDocument = org.bson.Document("name", "testDocument")
            .append("createdAt", System.currentTimeMillis())

        collection.insertOne(sampleDocument)

        println("Database '$databaseName' and collection '$collectionName' created successfully!")

        // Close the client
        mongoClient.close()
        */

    }


}

fun main() {
    val handler = DFSDataHandler()

}
