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
import com.mongodb.client.model.*
import com.mongodb.connection.ClusterSettings
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.bson.Document
import org.bson.conversions.Bson
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.impl.CoordinateArraySequence
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.text.SimpleDateFormat
import kotlin.collections.ArrayList

// This class is used to prepare the MongoDB for the benchmark.
// Updating collections, inserting data about regions, creating the collection and inserting data for flightTrips, creating indexes on and sharding collections
class DataHandler(databaseName: String) {

    private val conf: BenchmarkConfiguration
    private var database: MongoDatabase
    private var adminDatabase: MongoDatabase
    private val dateFormat = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    private var regions = listOf("municipalities", "counties", "districts")

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
                .credential(
                    MongoCredential.createCredential(
                        USER,
                        "admin",
                        PASSWORD.toCharArray()
                    )
                )
                .build())

        database = conn.getDatabase(databaseName)
        adminDatabase = conn.getDatabase("admin")
        listCollections(database)

    }

    fun updateDatabaseCollections() {
        val flightPointsCollection = database.getCollection("flightpoints")
        val citiesCollection = database.getCollection("cities")

        println("Updating flightpoints collection ... ")
        updateCollection(flightPointsCollection, arrayOf("timestamp", "coordinates"))

        flightPointsCollection.createIndex(
            Indexes.compoundIndex(
                Indexes.ascending("timestamp"),
                Indexes.ascending("flightId")
            ),
            IndexOptions().unique(false) // Shard keys typically are not unique
        )

        println("Compound index on 'timestamp' and 'flightId' created for collection flightpoints.")

        // Step 3: Shard the collection using the compound key
        adminDatabase.runCommand(
            Document("shardCollection", "${database.name}.flightpoints")
                .append("key", Document("timestamp", 1).append("flightId", 1))
        )

        println("Updating cities collection ... ")
        updateCollection(citiesCollection, arrayOf("coordinates"))
    }

    fun insertRegionalData(){

        println("Inserting regional data ...")
        for (region in regions) {
            val regionsCollection = database.getCollection(region)
            insertRegionalData(region, regionsCollection)
        }
    }

    fun createFlightTrips(){
        val flightPointsCollection = database.getCollection("flightpoints")
        val flightTripsCollection = database.getCollection("flighttrips")
        println("Creating flighttrips ...")
        createTripsCollection(flightPointsCollection, flightTripsCollection)
        println("Creating trajectories for flighttrips ...")
        createFlightTrajectories(flightTripsCollection)
        //println("Interpolating flighttrips ...")
        //interpolateFlightPoints(flightTripsCollection)
    }

    fun shardCollections(){

        val flightTripsCollection = database.getCollection("flighttrips")
        // create index on flightId and shard by it for collection flighttrips
        createIndexAndShard(flightTripsCollection, "flightId")

        // create index on hashed name and shard by it for municipalities, counties, and districts
        for (region in regions){
            val collection = database.getCollection(region)
            createIndexAndShard(collection, "name")
        }
    }

    fun createIndexes(){

        val flightPointsCollection = database.getCollection("flightpoints")
        val flightTripsCollection = database.getCollection("flighttrips")
        val flightPointsTsCollection = database.getCollection("flightpoints_ts")

        createTimeIndexes(flightPointsCollection) //Index on timestamp of flightpoints collection
        createSpatialIndexes(flightPointsCollection, flightPointsTsCollection, flightTripsCollection)
        createCompoundIndex(flightPointsCollection, flightPointsTsCollection)

//
//        flightTripsCollection.updateMany(
//            Document(),
//            Document("\$unset", Document("points", ""))
//        )
    }

    private fun createTimeIndexes(flightPointsCollection: MongoCollection<Document>){
        flightPointsCollection.createIndex(Document("timestamp",1))

        println("Created index on timestamp in ${flightPointsCollection.namespace.collectionName}.")
    }

    private fun createSpatialIndexes(flightPointsCollection: MongoCollection<Document>, flightPointsTsCollection: MongoCollection<Document>, flightTripsCollection: MongoCollection<Document>){
        flightPointsCollection.createIndex(Document("location","2dsphere"))
        flightPointsTsCollection.createIndex(Document("location","2dsphere"))
        flightTripsCollection.createIndex(Document("trajectory", "2dsphere"))

        println("Created index on spatial fields in ${flightPointsCollection.namespace.collectionName}, ${flightPointsTsCollection.namespace.collectionName} and ${flightTripsCollection.namespace.collectionName}.")


    }

    private fun createCompoundIndex(flightPointsCollection: MongoCollection<Document>, flightPointsTsCollection: MongoCollection<Document>) {
        var compoundIndex = Indexes.compoundIndex(
            Indexes.ascending("timestamp"), // Ascending index on timestamp
            Indexes.geo2dsphere("location"), // Geospatial index
        )

        var indexOptions = IndexOptions()

        flightPointsCollection.createIndex(compoundIndex, indexOptions)
        flightPointsTsCollection.createIndex(compoundIndex, indexOptions)

//        compoundIndex = Indexes.compoundIndex(
//            Indexes.geo2dsphere("points.location"),
//            Indexes.ascending("points.timestamp")
//        )
//        indexOptions = IndexOptions()
//
//        flightTripsCollection.createIndex(compoundIndex, indexOptions)
//
        println("Created compound index on timestamp and geom field in ${flightPointsCollection.namespace.collectionName} and ${flightPointsTsCollection.namespace.collectionName}.")
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
            listOf(lon, lat)
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
                    append("altitude", "\$altitude")
                    append("lon", "\$longitude")
                    append("lat", "\$latitude")
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

        // println("Starting the aggregation pipeline")
        flightPoints.aggregate(pipeline).toCollection()
        println("Aggregation complete, data has been inserted into the ${flightTrips.namespace.collectionName} collection.")
    }


    private fun interpolateFlightPoints(collection: MongoCollection<Document>, batchSize: Int = 10000) {
        // Use a cursor-based approach with batchSize
        val cursor = collection.find().batchSize(batchSize)

        val bulkOperations = mutableListOf<WriteModel<Document>>()
        var processedCount = 0

        cursor.forEach { flight ->
            val points = flight.getList("points", Document::class.java)
            val interpolatedPoints = mutableListOf<Document>()

            // Interpolate points within each flight
            for (i in 0 until points.size - 1) {
                val currentPoint = points[i]
                val nextPoint = points[i + 1]
                interpolatePointsBetween(currentPoint, nextPoint, interpolatedPoints)
            }
            interpolatedPoints.add(points.last())

            // Prepare the update operation for the interpolated points
            val update = Document("\$set", Document("points", interpolatedPoints))
            bulkOperations.add(UpdateOneModel(Document("_id", flight["_id"]), update))

            // Execute bulk write when batch size is reached
            if (bulkOperations.size >= batchSize) {
                collection.bulkWrite(bulkOperations)
                bulkOperations.clear()
                println("Executed bulk update for $batchSize documents.")
            }

            processedCount++
        }

        // Execute any remaining bulk operations
        if (bulkOperations.isNotEmpty()) {
            collection.bulkWrite(bulkOperations)
            println("Executed bulk update for remaining ${bulkOperations.size} documents.")
        }

        println("Processed $processedCount documents in total.")
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

    private fun createFlightTrajectories(collection: MongoCollection<Document>, batchSize: Int = 7500) {
        val geometryFactory = GeometryFactory()

        println("Starting Trajectory creation for all flights in the ${collection.namespace.collectionName} collection.")

        // Use a cursor-based approach with batchSize
        val cursor = collection.find().batchSize(batchSize)

        val bulkOperations = mutableListOf<WriteModel<Document>>()
        var processedCount = 0

        cursor.forEach { flight ->
            val points = flight.getList("points", Document::class.java)

            // Extract coordinates from the points
            val coordinates = points.mapNotNull { point ->
                //val location = point.get("location", Document::class.java)
                //val coords = location["coordinates"] as? List<*>
                val lon = point.getDouble("lon")
                val lat = point.getDouble("lat")
                Coordinate((lon).toDouble(), (lat).toDouble())
            }.toTypedArray()

            // Create a LineString if there are enough coordinates
            if (coordinates.size >= 2) {
                val lineString = geometryFactory.createLineString(CoordinateArraySequence(coordinates))

                // Convert LineString to GeoJSON format
                val geoJsonLineString = Document("type", "LineString")
                    .append("coordinates", lineString.coordinates.map { listOf(it.x, it.y) })

                // Prepare the update operation for the trajectory field
                val update = Updates.set("trajectory", geoJsonLineString)
                bulkOperations.add(UpdateOneModel(Document("_id", flight["_id"]), update))
            }

            // Execute bulk write when batch size is reached
            if (bulkOperations.size >= batchSize) {
                collection.bulkWrite(bulkOperations)
                bulkOperations.clear()
                println("Executed bulk update for $batchSize documents.")
            }

            processedCount++
        }

        // Execute any remaining operations
        if (bulkOperations.isNotEmpty()) {
            collection.bulkWrite(bulkOperations)
            println("Executed bulk update for remaining ${bulkOperations.size} documents.")
        }

        println("Trajectory creation completed for all flights of ${collection.namespace.collectionName}. Processed $processedCount documents.")
    }

    private fun createIndexAndShard(collection: MongoCollection<Document>, columnName: String) {
        val collectionName = collection.namespace.collectionName
        val databaseName = collection.namespace.databaseName

        if (collectionName == "flighttrips"){
            collection.createIndex(Document(columnName, 1))
            println("Index created on column '$columnName'.")
            adminDatabase.runCommand(
                Document("shardCollection", "$databaseName.$collectionName")
                    .append("key", Document(columnName, 1))
            )
            println("Collection '$collectionName' sharded on column '$columnName'.")


        }
        else {
            collection.createIndex(Document(columnName, "hashed"))
            println("Hashed index created on column '$columnName'.")
            adminDatabase.runCommand(
                Document("shardCollection", "$databaseName.$collectionName")
                    .append("key", Document(columnName, "hashed"))
            )
            println("Collection '$collectionName' sharded on hashed column '$columnName'.")
        }




    }

    private fun listCollections(database: MongoDatabase) {
        println("Collections in the database '${database.name}':")
        for (collectionName in database.listCollectionNames()) {
            println(collectionName)
        }
    }

    fun createTimeSeriesCollection(){
        val flightTrips = database.getCollection("flighttrips")
        val flightPointsTsCollection = database.getCollection("flightpoints_ts")
        migrateFlightPoints(flightTrips, flightPointsTsCollection, database)
    }

    private fun migrateFlightPoints(flightTripsCollection: MongoCollection<Document>, flightPointsTsCollection: MongoCollection<Document>, database: MongoDatabase) {

        try {

            val timeSeriesOptions = TimeSeriesOptions("timestamp").metaField("metadata").granularity(TimeSeriesGranularity.SECONDS)
            val createOptions = CreateCollectionOptions()
                .timeSeriesOptions(timeSeriesOptions)
            database.createCollection(flightPointsTsCollection.namespace.collectionName, createOptions)


            var indexOptions = IndexOptions()
            //val timestampIndex = Document("timestamp", 1)
//            flightPointsTsCollection.createIndex(
//                Document("metadata.flightId", 1).append("timestamp", 1),
//                indexOptions
//            )
//            val shardingCommand = Document("shardCollection", "${database.name}.${flightPointsTsCollection.namespace.collectionName}")
//                .append("key", Document("metadata.flightId", 1).append("timestamp", 1))

            val shardingCommand = Document("shardCollection", "${database.name}.${flightPointsTsCollection.namespace.collectionName}")
                .append("key", Document("timestamp", 1))


//            val shardingCommand = Document("shardCollection", "${database.name}.${flightPointsTsCollection.namespace.collectionName}")
//                .append("key", Document("timestamp", 1))

            adminDatabase.runCommand(shardingCommand)

            println("Collection '${flightPointsTsCollection.namespace.collectionName}' created with time-series settings.")

        } catch (e: Exception) {
            println("Error while resetting the collection: ${e.message}")
            return
        }

        val bulkOps = mutableListOf<WriteModel<Document>>()
        val batchSize = 200000
        val fetchBatchSize = 7500
        val projection = Projections.fields(
            Projections.include("flightId", "airplaneType", "destinationAirport", "originAirport", "track", "points")
        )


        try {

            flightTripsCollection.find().projection(projection).batchSize(fetchBatchSize).forEach { doc ->
                val flightId = doc.getInteger("flightId")
                val airplaneType = doc.getString("airplaneType")
                val destinationAirport = doc.getString("destinationAirport")
                val originAirport = doc.getString("originAirport")
                val track = doc.getInteger("track")
                val points = doc.getList("points", Document::class.java)

                // Preprocess all points at once
                val metadata = Document(
                    mapOf(
                        "flightId" to flightId,
                        "track" to track,
                        "originAirport" to originAirport,
                        "destinationAirport" to destinationAirport,
                        "airplaneType" to airplaneType,
                    )
                )

                val currentBatch = points.map { point ->
                    InsertOneModel(
                        Document(
                            mapOf(
                                "metadata" to metadata,
                                "timestamp" to point["timestamp"],
                                "lon" to point["lon"],
                                "lat" to point["lat"],
                                "altitude" to point["altitude"],
                            )
                        )
                    )
                }

                // Add preprocessed operations to the bulkOps
                bulkOps.addAll(currentBatch)

                // Execute bulk write when batch size is reached
                if (bulkOps.size >= batchSize) {
                    flightPointsTsCollection.bulkWrite(bulkOps)
                    bulkOps.clear() // Clear the bulkOps list
                }
            }

// Final batch write for remaining operations
            if (bulkOps.isNotEmpty()) {
                flightPointsTsCollection.bulkWrite(bulkOps)
            }

//            flightTripsCollection.find().projection(projection).batchSize(fetchBatchSize).forEach { doc ->
//                val flightId = doc.getInteger("flightId")
//                val airplaneType = doc.getString("airplaneType")
//                val destinationAirport = doc.getString("destinationAirport")
//                val originAirport = doc.getString("originAirport")
//                val track = doc.getInteger("track")
//                val points = doc.getList("points", Document::class.java)
//
//                points.forEach { point ->
//                    val bulkOp = InsertOneModel(
//                        Document(mapOf(
//                            // metadata entries
//                            "metadata" to Document(
//                                mapOf(
//                                    "flightId" to flightId,
//                                    "track" to track,
//                                    "originAirport" to originAirport,
//                                    "destinationAirport" to destinationAirport,
//                                    "airplaneType" to airplaneType,
//                                )
//                            ),
//                            "timestamp" to point["timestamp"],
//                            "lon" to point["lon"],
//                            "lat" to point["lat"],
//                            "altitude" to point["altitude"],
//                        ))
//                    )
//                    bulkOps.add(bulkOp)
//
//                    // Execute bulk write when batch size is reached
//                    if (bulkOps.size >= batchSize) {
//                        flightPointsTsCollection.bulkWrite(bulkOps)
//                        bulkOps.clear() // Clear the bulkOps list
//                    }
//                }
//            }
//            println("Time-series data migrated successfully!")
//
//            // Execute any remaining operations
//            if (bulkOps.isNotEmpty()) {
//                flightPointsTsCollection.bulkWrite(bulkOps)
//            }

            val densifyStage = Document(
                "\$densify", Document()
                    .append("field", "timestamp")
                    .append("partitionByFields", listOf("metadata.flightId", "metadata.track"))
                    .append(
                        "range", Document()
                            .append("step", 1)
                            .append("unit", "second")
                            .append("bounds", "partition")
                    )
            )

            // Step 2: $fill stage
            val fillStage = Document(
                "\$fill", Document()
                    .append("partitionByFields", listOf("metadata.flightId", "metadata.track"))
                    .append(
                        "sortBy", Document("timestamp", 1)
                    )
                    .append(
                        "output", Document()
                            .append("altitude", Document("method", "linear"))
                            .append("lon", Document("method", "linear"))
                            .append("lat", Document("method", "linear"))
                    )
            )


            // Execute the pipeline
            val pipeline = listOf(densifyStage, fillStage)
            val result = flightPointsTsCollection.aggregate(pipeline).toList()

            // TODO: transform coordinates to GeoJSON
            val updateCoordinates = Document("\$set", mapOf(
                "location" to Document(
                    "type", "Point"
                ).append(
                    "coordinates", listOf("\$lon", "\$lat")
                )
            ))

            val updateLocationsPipeline = mutableListOf<Bson>()

            updateLocationsPipeline.add(updateCoordinates)

            println("Creating GeoJSON fields for ${flightPointsTsCollection.namespace.collectionName}.")
            flightPointsTsCollection.updateMany(Document(), pipeline)

        } catch (e: Exception) {
            println("Error during migration: ${e.message}")
        }
    }


    fun processFlightTripsByRange(
        flightTripsCollection: MongoCollection<Document>,
        flightPointsTsCollection: MongoCollection<Document>,
        batchSize: Int,
        flightIdThreshold: Int
    ) {
        runBlocking {
            // Launch two coroutines for parallel processing
            val largerFlightIdsJob = launch {
                processFlightTrips(flightTripsCollection, flightPointsTsCollection, batchSize, flightIdThreshold, true)
            }

            val smallerFlightIdsJob = launch {
                processFlightTrips(flightTripsCollection, flightPointsTsCollection, batchSize, flightIdThreshold, false)
            }

            // Wait for both coroutines to finish
            largerFlightIdsJob.join()
            smallerFlightIdsJob.join()
        }
    }

    suspend fun processFlightTrips(
        flightTripsCollection: MongoCollection<Document>,
        flightPointsTsCollection: MongoCollection<Document>,
        batchSize: Int,
        flightIdThreshold: Int,
        isLarger: Boolean
    ) {
        val filter = if (isLarger) {
            Document("flightId", Document("\$gt", flightIdThreshold))
        } else {
            Document("flightId", Document("\$lte", flightIdThreshold))
        }

        val bulkOps = mutableListOf<InsertOneModel<Document>>()

        flightTripsCollection.find(filter).batchSize(batchSize).forEach { doc ->
            val flightId = doc.getInteger("flightId")
            val airplaneType = doc.getString("airplaneType")
            val destinationAirport = doc.getString("destinationAirport")
            val originAirport = doc.getString("originAirport")
            val track = doc.getInteger("track")
            val points = doc.getList("points", Document::class.java)

            val metadata = Document(
                mapOf(
                    "flightId" to flightId,
                    "track" to track,
                    "originAirport" to originAirport,
                    "destinationAirport" to destinationAirport,
                    "airplaneType" to airplaneType,
                )
            )

            val currentBatch = points.map { point ->
                InsertOneModel(
                    Document(
                        mapOf(
                            "metadata" to metadata,
                            "timestamp" to point["timestamp"],
                            "lon" to point["lon"],
                            "lat" to point["lat"],
                            "altitude" to point["altitude"],
                        )
                    )
                )
            }

            bulkOps.addAll(currentBatch)

            if (bulkOps.size >= batchSize) {
                flightPointsTsCollection.bulkWrite(bulkOps)
                bulkOps.clear()
            }
        }

        if (bulkOps.isNotEmpty()) {
            flightPointsTsCollection.bulkWrite(bulkOps)
        }
    }





}




fun main() {
    val handler = DataHandler("aviation_data")
    handler.updateDatabaseCollections()
    handler.shardCollections()
    handler.insertRegionalData()
    handler.createFlightTrips()
    handler.createTimeSeriesCollection()
    handler.createIndexes()
    //handler.runQueries()

}









