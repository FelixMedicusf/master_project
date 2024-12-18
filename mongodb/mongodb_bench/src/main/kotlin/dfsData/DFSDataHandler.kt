import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.KotlinFeature
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.mongodb.MongoClientSettings
import com.mongodb.MongoCredential
import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.UpdateOneModel
import com.mongodb.client.model.Updates
import com.mongodb.client.model.WriteModel
import com.mongodb.connection.ClusterSettings
import org.bson.Document
import org.bson.conversions.Bson
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.impl.CoordinateArraySequence
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths

class DFSDataHandler(databaseName: String) {

    private val conf: BenchmarkConfiguration
    private var database: MongoDatabase

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
        val mongodbClientPort = 27017

        var mongodbHosts = ArrayList<ServerAddress>();

        mongodbIps.forEach{ipAddress -> mongodbHosts.add(ServerAddress(ipAddress, mongodbClientPort))}

        val conn = MongoClients.create(
            MongoClientSettings.builder()
                .applyToClusterSettings { builder: ClusterSettings.Builder ->
                    builder.hosts(
                        mongodbHosts
                    )
                }
                .readPreference(ReadPreference.nearest())
                .credential(
                    MongoCredential.createCredential(
                        user,
                        "admin",
                        password.toCharArray()
                    )
                )
                .build())

        database = conn.getDatabase(databaseName)
        listCollections(database)

    }

    fun updateDatabaseCollections() {
        val flightPointsCollection = database.getCollection("flightpoints")
        val citiesCollection = database.getCollection("cities")

        updateCollection(flightPointsCollection, arrayOf("timestamp", "coordinates"))
        updateCollection(citiesCollection, arrayOf("coordinates"))
    }

    fun insertRegionalData(){
        for (region in listOf("districts", "counties", "municipalities")) {
            val regionsCollection = database.getCollection(region)
            insertRegionalData(region, regionsCollection)
        }
    }

    fun createFlightTrips(){
        val flightTripsCollection = database.getCollection("flighttrips")
        val flightPointsCollection = database.getCollection("flightpoints")

        createTripsCollection(flightPointsCollection, flightTripsCollection)
        interpolateFlightPoints(flightTripsCollection)
        createFlightTrajectories(flightTripsCollection)
    }

    private fun updateCollection(collection: MongoCollection<Document>, fields: Array<String>) {

        val updateTimestamps = Document("\$set", mapOf(
            "timestamp" to Document(
                "\$dateFromString", mapOf(
                    "dateString" to "\$timestamp",
                    "format" to "%d-%m-%Y %H:%M:%S"
                )
            )
        ))

        val updateCoordinates = Document("\$set", mapOf(
            "location" to Document(
                "type", "Point"
            ).append(
                "coordinates", listOf("\$longitude", "\$latitude")
            )
        ))

        val pipeline = mutableListOf<Bson>()
        if (fields.contains("timestamp")){
            pipeline.add(updateTimestamps)
        }
        if (fields.contains("coordinates")){
            pipeline.add(updateCoordinates)
        }
        collection.updateMany(Document(), pipeline)
        println("Collection ${collection.namespace.collectionName} updated successfully!")
    }


    private fun insertRegionalData(collectionName: String, collection: MongoCollection<Document>){
        val resourceFiles = mapOf(
            "counties" to "src/main/resources/counties-wkt.csv",
            "districts" to "src/main/resources/districts-wkt.csv",
            "municipalities" to "src/main/resources/municipalities-wkt.csv"
        )

        val filePath = resourceFiles[collectionName]
            ?: throw IllegalArgumentException("Invalid collection name: $collectionName")

        println("Reading data from: $filePath")

        File(filePath).useLines { lines ->
            lines.drop(1).forEach { line ->
                val parts = line.split(";")

                if (parts.size >= 2) {
                    val name = parts[0].trim()
                    val wktPolygon = parts[1].trim()

                    try {
                        val geoJsonPolygon = wktToGeoJson(wktPolygon)

                        val document = Document("name", name)
                            .append("polygon", geoJsonPolygon)

                        collection.insertOne(document)

                    } catch (e: Exception) {
                        println("Error processing line: $line, error: ${e.message}")
                    }
                } else {
                    println("Skipping invalid line: $line")
                }
            }
        }
        println("All data has been loaded into the $collectionName collection.")
    }

    private fun wktToGeoJson(wkt: String): Document {

        if (!wkt.startsWith("POLYGON")) {
            throw IllegalArgumentException("Invalid WKT format: $wkt")
        }

        val coordinatesString = wkt.replace("POLYGON((", "").replace("))", "")
        val coordinates = coordinatesString.split(", ").map { point ->
            val (lon, lat) = point.split(" ").map { it.toDouble() }
            listOf(lon, lat) // GeoJSON uses [longitude, latitude] order
        }

        return Document("type", "Polygon")
            .append("coordinates", listOf(coordinates))
    }

    private fun createTripsCollection(flightPoints: MongoCollection<Document>, flightTrips: MongoCollection<Document>) {

        val pipeline = listOf(
            Document("\$sort", Document("timestamp", 1)),
            Document("\$group", Document().apply {
                append("_id", Document().apply {
                    append("flightId", "\$flightId")
                    append("originAirport", "\$originAirport")
                    append("destinationAirport", "\$destinationAirport")
                    append("airplaneType", "\$airplaneType")
                    append("track", "\$track")
                })
                append("points", Document("\$push", Document().apply {
                    append("timestamp", "\$timestamp")
                    append("location", Document().apply {
                        append("type", "Point")
                        append("coordinates", listOf("\$longitude", "\$latitude"))
                    })
                    append("altitude", "\$altitude") // Include altitude
                }))
            }),
            Document("\$addFields", Document().apply {
                append("flightId", "\$_id.flightId")
                append("originAirport", "\$_id.originAirport")
                append("destinationAirport", "\$_id.destinationAirport")
                append("airplaneType", "\$_id.airplaneType")
                append("track", "\$_id.track")
            }),
            Document("\$project", Document().apply {
                append("_id", 0)
                append("flightId", 1)
                append("originAirport", 1)
                append("destinationAirport", 1)
                append("airplaneType", 1)
                append("track", 1)
                append("points", 1)
            }),
            Document("\$merge", Document().apply {
                append("into", flightTrips.namespace.collectionName)
                append("whenMatched", "merge")
                append("whenNotMatched", "insert")
            })
        )


        println("Starting the aggregation pipeline")
        flightPoints.aggregate(pipeline).toCollection()
        println("Aggregation complete, data has been inserted into the ${flightTrips.namespace.collectionName} collection.")
    }

    private fun interpolateFlightPoints(collection: MongoCollection<Document>, batchSize: Int = 8000) {
        var skip = 0

        while (true) {

            val flights = collection.find()
                .skip(skip)
                .limit(batchSize)
                .toList()

            if (flights.isEmpty()){
                break
            }

            val bulkOperations = mutableListOf<WriteModel<Document>>()

            for (flight in flights) {
                val points = flight.getList("points", Document::class.java)
                val interpolatedPoints = mutableListOf<Document>()

                for (i in 0..<points.size - 1) {
                    val currentPoint = points[i]
                    val nextPoint = points[i + 1]
                    interpolatePointsBetween(currentPoint, nextPoint, interpolatedPoints)
                }
                interpolatedPoints.add(points.last())
                val update = Document("\$set", Document("points", interpolatedPoints))
                bulkOperations.add(UpdateOneModel(Document("_id", flight["_id"]), update))

            }

            if (bulkOperations.isNotEmpty()) {
                collection.bulkWrite(bulkOperations)
                println("Executed bulk update for $batchSize documents.")
            }

            skip += batchSize
            println("Processed $batchSize documents, skipping $skip next.")
        }
    }
    private fun interpolatePointsBetween(currentPoint: Document, nextPoint: Document, result: MutableList<Document>) {
        val currentTimestamp = currentPoint.getDate("timestamp").toInstant()
        val nextTimestamp = nextPoint.getDate("timestamp").toInstant()

        val currentLocation = currentPoint.get("location", Document::class.java)
        val nextLocation = nextPoint.get("location", Document::class.java)

        val currentCoordinatesList = currentLocation["coordinates"] as List<*>
        val nextCoordinatesList = nextLocation["coordinates"] as List<*>

        val currentLongitude = (currentCoordinatesList[0] as Number).toDouble()
        val currentLatitude = (currentCoordinatesList[1] as Number).toDouble()
        val nextLongitude = (nextCoordinatesList[0] as Number).toDouble()
        val nextLatitude = (nextCoordinatesList[1] as Number).toDouble()

        val currentAltitude = currentPoint.getDouble("altitude")
        val nextAltitude = nextPoint.getDouble("altitude")

        var interpolatedTimestamp = currentTimestamp.plusSeconds(1)

        result.add(currentPoint)

        while (interpolatedTimestamp.isBefore(nextTimestamp)) {
            val currentSeconds = currentTimestamp.epochSecond
            val nextSeconds = nextTimestamp.epochSecond
            val interpolatedSeconds = interpolatedTimestamp.epochSecond

            val interpolatedLongitude = currentLongitude + (nextLongitude - currentLongitude) * (interpolatedSeconds - currentSeconds) / (nextSeconds - currentSeconds)
            val interpolatedLatitude = currentLatitude + (nextLatitude - currentLatitude) * (interpolatedSeconds - currentSeconds) / (nextSeconds - currentSeconds)
            val interpolatedAltitude = currentAltitude + (nextAltitude - currentAltitude) * (interpolatedSeconds - currentSeconds) / (nextSeconds - currentSeconds)

            result.add(Document().apply {
                append("timestamp", java.util.Date.from(interpolatedTimestamp))
                append("location", Document("type", "Point").append("coordinates", listOf(interpolatedLongitude, interpolatedLatitude)))
                append("altitude", interpolatedAltitude)
            })
            interpolatedTimestamp = interpolatedTimestamp.plusSeconds(1)
        }
    }

    private fun createFlightTrajectories(collection: MongoCollection<Document>, batchSize: Int = 8000) {
        val geometryFactory = GeometryFactory()
        var skip = 0

        println("Starting Trajectory creation for all flights in the ${collection.namespace.collectionName} collection.")

        while (true) {
            // Fetch a batch of documents
            val flights = collection.find()
                .skip(skip)
                .limit(batchSize)
                .toList()

            if (flights.isEmpty()){
                break
            }
            val bulkOperations = mutableListOf<WriteModel<Document>>()

            for (flight in flights) {

                val points = flight.getList("points", Document::class.java)

                val coordinates = points.mapNotNull { point ->
                    val location = point.get("location", Document::class.java)
                    val coords = location["coordinates"] as? List<*>
                    coords?.let { Coordinate((it[0] as Number).toDouble(), (it[1] as Number).toDouble()) }
                }.toTypedArray()

                if (coordinates.size >= 2) {
                    val lineString = geometryFactory.createLineString(CoordinateArraySequence(coordinates))

                    val geoJsonLineString = Document("type", "LineString")
                        .append("coordinates", lineString.coordinates.map { listOf(it.x, it.y) })

                    val update = Updates.set("trajectory", geoJsonLineString)
                    bulkOperations.add(UpdateOneModel(Document("_id", flight["_id"]), update))
                }
            }

            if (bulkOperations.isNotEmpty()) {
                collection.bulkWrite(bulkOperations)
                println("Executed bulk update for $batchSize documents.")
            }

            skip += batchSize
            println("Processed $batchSize documents, skipping $skip next.")
        }

        println("Trajectory creation completed for all flights of ${collection.namespace.collectionName}.")
    }

    private fun listCollections(database: MongoDatabase) {
        println("Collections in the database '${database.name}':")
        for (collectionName in database.listCollectionNames()) {
            println(collectionName)
        }
    }
}

fun main() {
    val handler = DFSDataHandler("aviation_data")
    handler.updateDatabaseCollections()
    handler.insertRegionalData()
    handler.createFlightTrips()
}
