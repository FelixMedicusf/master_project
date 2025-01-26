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
import kotlinx.coroutines.*
import org.bson.Document
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.impl.CoordinateArraySequence
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.time.Instant
import kotlin.collections.ArrayList
import kotlin.coroutines.coroutineContext

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

    private fun listCollections(database: MongoDatabase) {
        println("Collections in the database '${database.name}':")
        for (collectionName in database.listCollectionNames()) {
            println(collectionName)
        }
    }

    fun updateDatabaseCollections() {
        val flightPointsCollection = database.getCollection("flightpoints")
        val citiesCollection = database.getCollection("cities")
        val airportsCollection = database.getCollection("airports")

        // Flightpoints index creation
        flightPointsCollection.createIndex(Document("flightId", 1))

        adminDatabase.runCommand(
            Document("shardCollection", "${database.name}.flightpoints")
                .append("key", Document("flightId", 1))
        )
        println("Created index on 'flightId' for collection flightpoints and sharded on that index.")

        println("Wait for rebalancing of documents in flightpoints collection.")
        Thread.sleep(25_000)

        println("Updating flightpoints collection (creating timestamps and GeoJson) ... ")

        val combinedUpdate =
            listOf(
                Document("\$set", mapOf(
                    "timestamp" to Document("\$dateFromString", mapOf(
                        "dateString" to "\$timestamp",
                        "format" to "%d-%m-%Y %H:%M:%S"
                    )),
                    "location" to Document("type", "Point").append(
                        "coordinates", listOf("\$longitude", "\$latitude")
                    )
                ))
            )


        val separators = listOf(0, 700076001, 732177911, 735346091, 745304441, 774441640)
        val coroutineNumber = separators.size-1

        runBlocking {

            val updateJobs = (0 until coroutineNumber).map { index ->
                launch(Dispatchers.IO) {
                    try {
                        val flightIdLowerBound = separators[index]
                        val flightIdUpperBound = separators[index + 1]
                        println("$index: Update flightpoints collection")

                        val filter = Document("\$and", listOf(
                            Document("flightId", Document("\$gt", flightIdLowerBound).append("\$lte", flightIdUpperBound)), // Flight ID range
                            Document("timestamp", Document("\$exists", true)), // Ensure 'timestamp' exists
                            Document("timestamp", Document("\$type", "string")) // Ensure 'timestamp' is a string
                        ))

                        retryWriteOperation {
                            flightPointsCollection.updateMany(filter, combinedUpdate)
                        }

                        println("$index finished updating flightpoint documents.")

                    }catch (e: Exception){

                        println("Error updating flightpoints in coroutine: ${e.message}")
                    }
                }
            }

            updateJobs.joinAll()
        }

        flightPointsCollection.createIndex(Document("timestamp", 1))
        flightPointsCollection.createIndex(Document("location","2dsphere"))

        var compoundIndex = Indexes.compoundIndex(
            Indexes.ascending("timestamp"), // Ascending index on timestamp
            Indexes.geo2dsphere("location"), // Geospatial index
        )
        var indexOptions = IndexOptions()
        flightPointsCollection.createIndex(compoundIndex, indexOptions)

        println("Created indexes for flightpoints collection.")

        val locationUpdate =
            listOf(
                Document("\$set", mapOf(
                    "location" to Document("type", "Point").append(
                        "coordinates", listOf("\$longitude", "\$latitude")
                    )
                ))
            )

        println("Updating cities collection ... ")
        citiesCollection.updateMany(Document(),locationUpdate)
        citiesCollection.createIndex(Document("name", "hashed"))
        citiesCollection.createIndex(Document("location", "2dsphere"))

        println("Creating indexes for airports collection.")
        airportsCollection.createIndex(Document("City", "hashed"))
        airportsCollection.createIndex(Document("ICAO", "hashed"))


    }

    private suspend fun retryWriteOperation(retries: Int = 50, block: suspend () -> Unit) {
        repeat(retries) {
            try {
                block()
                return // Exit on success
            } catch (e: Exception) {
                if (it == retries - 1) throw e // Re-throw on final attempt
                println("Retrying operation due to error: ${e.message}")
                Thread.sleep(1000)
            }
        }
    }

    fun insertRegionalData(){
        println("Inserting regional data ...")
        for (region in regions) {
            val regionsCollection = database.getCollection(region)
            insertRegionalData(region, regionsCollection)
        }
    }

    private fun insertRegionalData(collectionName: String, collection: MongoCollection<Document>) {
        val resourceFiles = mapOf(
            "counties" to "counties-wkt.csv",
            "districts" to "districts-wkt.csv",
            "municipalities" to "municipalities-wkt.csv"
        )

        val resourcePath = resourceFiles[collectionName]
            ?: throw IllegalArgumentException("Invalid collection name: $collectionName")

        println("Reading data from resource: $resourcePath")

        val resourceStream = this::class.java.classLoader.getResourceAsStream(resourcePath)
            ?: throw IllegalArgumentException("Resource not found: $resourcePath")

        resourceStream.bufferedReader().use { reader ->
            reader.lineSequence()
                .drop(1) // Skip the header line
                .forEach { line ->
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

    private fun createIndexAndShard(collection: MongoCollection<Document>, columnName: String) {
        val collectionName = collection.namespace.collectionName
        val databaseName = collection.namespace.databaseName

        if (collectionName == "flighttrips"){
            collection.createIndex(Document(columnName, 1))
            adminDatabase.runCommand(
                Document("shardCollection", "$databaseName.$collectionName")
                    .append("key", Document(columnName, 1))
            )
            println("Collection '$collectionName' sharded on column '$columnName'.")
        }
        else {
            println("Creating indexes for $collectionName.")
            collection.createIndex(Document("name", "hashed"))
            collection.createIndex(Document("polygon", "2dsphere"))

            // TODO: Implement full replication for static collections among all shard servers
//            adminDatabase.runCommand(
//                Document("shardCollection", "$databaseName.$collectionName")
//                    .append("key", Document(columnName, "hashed"))
//            )
//            println("Collection '$collectionName' sharded on hashed column '$columnName'.")
        }
    }

    fun createFlightTrips(){
        val flightPointsCollection = database.getCollection("flightpoints")
        val flightTripsCollection = database.getCollection("flighttrips")
        println("Creating flighttrips ...")

        val separators = listOf(0, 700642631, 710076001, 718926541, 728177911, 736845861, 743346091, 754447851, 760302441, 773383481, 774441640)
        val coroutineNumber = separators.size - 1
        runBlocking {

            val createFlightsJobs = (0 until coroutineNumber).map { index ->
                launch(Dispatchers.IO) {
                    try {
                        val flightIdLowerBound = separators[index]
                        val flightIdUpperBound = separators[index + 1]
                        println("$index: Started creating flights for flighttrips collection.")
                        createTripsCollection(flightPointsCollection, flightTripsCollection, flightIdLowerBound, flightIdUpperBound)
                        println("$index: Finished creating flights for flighttrips collection.")

                    }catch (e: Exception){
                        println("Error in trajectory creation coroutine: ${e.message}")
                    }
                }
            }
            createFlightsJobs.joinAll()
        }
    }

    private fun createTripsCollection(flightPointsCollection: MongoCollection<Document>, flightTripsCollection: MongoCollection<Document>, flightIdLowerBound: Int, flightIdUpperBound: Int) {

        val filter = Document("flightId", Document().append("\$gt", flightIdLowerBound).append("\$lte", flightIdUpperBound))

        val pipeline = listOf(
            Document("\$match", Document(filter)),
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
                append("into", flightTripsCollection.namespace.collectionName)
                append("whenMatched", "merge")
                append("whenNotMatched", "insert")
            })
        )

        flightPointsCollection.aggregate(pipeline).toCollection()

    }

    fun createTrajectories(
        flightIdThresholds: List<Int>,
        batchSize: Int = 12500
    ) {

        val flightTripsCollection = database.getCollection("flighttrips")
        val coroutineNumber = flightIdThresholds.size - 1

        runBlocking {

            val createTrajsJobs = (0 until coroutineNumber).map { index ->
                launch(Dispatchers.IO) {
                    try {

                        val flightIdLowerBound = flightIdThresholds[index]
                        val flightIdUpperBound = flightIdThresholds[index + 1]
                        println("$index: Started creating trajectories for flighttrips collection for $flightIdLowerBound < flightId <= $flightIdUpperBound.")
                        createFlightTrajectories(flightTripsCollection, flightIdLowerBound, flightIdUpperBound, batchSize)
                        println("$index: Finished creating trajectories for flighttrips collection.")

                    } catch (e: Exception) {
                        println("Error in trajectory creation coroutine: ${e.message}")
                    }
                }
            }
            createTrajsJobs.joinAll()
        }

        println("Deleting points arrays in flighttrips collection.")
        flightTripsCollection.updateMany(
            Document(),
            Document("\$unset", Document("points", ""))
        )

        flightTripsCollection.createIndex(Document("trajectory", "2dsphere"))

    }

    private fun createFlightTrajectories(
        flightTripsCollection: MongoCollection<Document>,
        flightIdLowerBound: Int = 0,
        flightIdUpperBound: Int = Int.MAX_VALUE,
        batchSize: Int
    ) {

        val geometryFactory = GeometryFactory()

        val filter = Document("flightId", Document().append("\$gt", flightIdLowerBound).append("\$lte", flightIdUpperBound))

        val cursor = flightTripsCollection.find(filter).batchSize(batchSize)

        val bulkOperations = mutableListOf<WriteModel<Document>>()
        var processedCount = 0

        cursor.forEach { flight ->
            val points = flight.getList("points", Document::class.java)

            // Extract coordinates from the points
            val coordinates = points.mapNotNull { point ->
                val lon = point.getDouble("lon")
                val lat = point.getDouble("lat")
                Coordinate((lon).toDouble(), (lat).toDouble())
            }.toTypedArray()

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
                flightTripsCollection.bulkWrite(bulkOperations)
                bulkOperations.clear()
                println("Executed bulk update for $batchSize documents (trajectory creation).")
            }

            processedCount++
        }

        // Execute any remaining operations
        if (bulkOperations.isNotEmpty()) {
            flightTripsCollection.bulkWrite(bulkOperations)
            println("Executed bulk update for remaining ${bulkOperations.size} documents (trajectory creation).")
        }

        println("Processed $processedCount documents (trajectory creation).")

    }

    fun createFlightsPointsTs(
        flightIdThresholds: List<Int>
    ){

        val timeSeriesOptions = TimeSeriesOptions("timestamp").metaField("metadata").granularity(TimeSeriesGranularity.SECONDS)
        val createOptions = CreateCollectionOptions().timeSeriesOptions(timeSeriesOptions)
        val flightPointsCollection = database.getCollection("flightpoints")
        val flightPointsTsCollection = database.getCollection("flightpoints_ts")

        database.createCollection(flightPointsTsCollection.namespace.collectionName, createOptions)
        println("Collection '${flightPointsTsCollection.namespace.collectionName}' created with time-series settings.")

        val shardingCommand = Document("shardCollection", "${database.name}.${flightPointsTsCollection.namespace.collectionName}")
            .append("key", Document("timestamp", 1))

        adminDatabase.runCommand(shardingCommand)

        val june1 = Instant.parse("2023-06-01T00:00:00Z")
        val september1 = Instant.parse("2023-09-01T00:00:00Z")

        val splitRanges = listOf(
            Document("split", "${database.name}.system.buckets.flightpoints_ts")
                .append("middle", Document("control.min.timestamp",  june1)), // MinKey -> June 1

            Document("split", "${database.name}.system.buckets.flightpoints_ts")
                .append("middle", Document("control.min.timestamp", september1))  // June 1 -> September 1
        )

        splitRanges.forEach { splitCommand ->
            adminDatabase.runCommand(splitCommand)
        }

        val may1 = Instant.parse("2023-05-01T00:00:00Z")
        val july1 = Instant.parse("2023-07-01T00:00:00Z")
        val october1 = Instant.parse("2023-10-01T00:00:00Z")

        val moveChunkCommands = listOf(
            Document("moveChunk", "${database.name}.system.buckets.flightpoints_ts")
                .append("find", Document("control.min.timestamp", may1)) // MinKey -> June 1
                .append("to", "shard1ReplSet"),

            Document("moveChunk", "${database.name}.system.buckets.flightpoints_ts")
                .append("find", Document("control.min.timestamp", july1)) // June 1 -> September 1
                .append("to", "shard2ReplSet"),

            Document("moveChunk", "${database.name}.system.buckets.flightpoints_ts")
                .append("find", Document("control.min.timestamp", october1)) // September 1 -> MaxKey
                .append("to", "shard3ReplSet")
        )

        moveChunkCommands.forEach { moveChunkCommand ->
            adminDatabase.runCommand(moveChunkCommand)
        }

        println("Sharded ${flightPointsTsCollection.namespace.collectionName} on timestamp and created chunks for shards.")

        val coroutineNumber = flightIdThresholds.size - 1

        runBlocking {

            val insertJobs = (0 until coroutineNumber).map { index ->
                launch(Dispatchers.IO) {
                    try {
                        val flightIdLowerBound = flightIdThresholds[index]
                        val flightIdUpperBound = flightIdThresholds[index + 1]
                        println("$index: Started migrating flightpoints to flightpoints_ts for $flightIdLowerBound < flightId <= $flightIdUpperBound.")
                        migrateFlightPoints(flightPointsCollection, flightPointsTsCollection, flightIdLowerBound, flightIdUpperBound, 250_000)
                        println("$index: Finished migrating flightpoints to flightpoints_ts for $flightIdLowerBound < flightId <= $flightIdUpperBound.")

                    }catch (e: Exception){
                        println("Error in migration coroutine: ${e.message}")
                    }
                }
            }

            insertJobs.joinAll()
        }


        println("Creating index on flightId on flightpoints_ts collection.")
        flightPointsTsCollection.createIndex(Document("metadata.flightId", 1))

        var indexOptions = IndexOptions()
        println("Creating compound index (flightid, timestamp) on flightpoints_ts collection.")

        flightPointsTsCollection.createIndex(
            Document("metadata.flightId", 1).append("timestamp", 1),
            indexOptions
        )

        flightPointsTsCollection.createIndex(
            Document("metadata.flightId", 1).append("metadata.track", 1).append("timestamp", 1),
            indexOptions
        )



        runBlocking {
            val interpolateJobs = (0 until coroutineNumber).map { index ->
                val flightIdLowerBound = flightIdThresholds[index]
                val flightIdUpperBound = flightIdThresholds[index + 1]
                launch(Dispatchers.IO) {
                    println("$index: Started migrating flightpoints to flightpoints_ts for $flightIdLowerBound < flightId <= $flightIdUpperBound.")
                    interpolateFlightPointsTs(flightPointsTsCollection, flightIdLowerBound, flightIdUpperBound, 500000)
                    println("$index: Finished migrating flightpoints to flightpoints_ts for $flightIdLowerBound < flightId <= $flightIdUpperBound.")
                }
            }
            interpolateJobs.joinAll()
        }
    }

    private suspend fun migrateFlightPoints(
        flightPointsCollection: MongoCollection<Document>,
        flightPointsTsCollection: MongoCollection<Document>,
        flightIdLowerBound: Int = 0,
        flightIdUpperBound: Int = Int.MAX_VALUE,
        batchSize: Int = 200_000
    ) {

        val filter = Document("flightId", Document().append("\$gt", flightIdLowerBound).append("\$lte", flightIdUpperBound))

        val bulkOps = mutableListOf<WriteModel<Document>>()

        var processedCount = 0

        try {

            flightPointsCollection.find(filter).batchSize(batchSize).forEach { doc ->
                val flightId = doc.getInteger("flightId")
                val destinationAirport = doc.getString("destinationAirport")
                val originAirport = doc.getString("originAirport")
                val track = doc.getInteger("track")
                val timestamp = doc.getDate("timestamp")
                val altitude = doc.getDouble("altitude")
                val location = doc.get("location", Document::class.java)

                // Prepare metadata fields
                val metadata = Document(
                    mapOf(
                        "flightId" to flightId,
                        "destinationAirport" to destinationAirport,
                        "originAirport" to originAirport,
                        "track" to track
                    )
                )

                val timeSeriesDoc = Document(
                    mapOf(
                        "metadata" to metadata,
                        "timestamp" to timestamp,
                        "altitude" to altitude,
                        "location" to location
                    )
                )

                bulkOps.add(InsertOneModel(timeSeriesDoc))

                if (bulkOps.size >= batchSize) {
                    flightPointsTsCollection.bulkWrite(bulkOps, BulkWriteOptions().ordered(false))
                    bulkOps.clear()
                    println("Conducted Bulk Op for $batchSize Docs in context: ${coroutineContext[Job]} (migrating flightpoints to time-series collection).")// Clear the bulkOps list
                }

                processedCount++
            }

            if (bulkOps.isNotEmpty()) {
                flightPointsTsCollection.bulkWrite(bulkOps)
                println("Executed bulk update for remaining ${bulkOps.size} documents.")
            }

            println("Inserted $processedCount documents into flightpoints_ts collection ${coroutineContext[Job]}.")

        } catch (e: Exception) {
            println("Error during migration: ${e.message}")
        }
    }


    private fun interpolateFlightPointsTs(
        flightPointsTsCollection: MongoCollection<Document>,
        flightIdLowerBound: Int = 0,
        flightIdUpperBound: Int = Int.MAX_VALUE,
        batchSize: Int = 300000
    ) {
        val filter = Document("metadata.flightId", Document().append("\$gt", flightIdLowerBound).append("\$lte", flightIdUpperBound))

        val batchSizeGroupedDocs = 15000
        // Use aggregation to group by flightId and track
        val cursor = flightPointsTsCollection.aggregate(
            listOf(
                // Add the $match stage for filtering by flightId
                Document("\$match", filter),

                // Sorting stage
                Document("\$sort", Document("metadata.flightId", 1)
                    .append("metadata.track", 1)
                    .append("timestamp", 1)),

                // Grouping stage
                Document("\$group", Document()
                    .append("_id", Document("flightId", "\$metadata.flightId")
                        .append("track", "\$metadata.track").append("destinationAirport", "\$metadata.destinationAirport")
                        .append("originAirport", "\$metadata.originAirport").append("airplaneType", "\$metadata.airplaneType"))
                    .append("points", Document("\$push", Document("timestamp", "\$timestamp")
                        .append("location", "\$location")
                        .append("altitude", "\$altitude"))))
            )
        ).allowDiskUse(true).batchSize(batchSizeGroupedDocs)

        val bulkOperations = mutableListOf<WriteModel<Document>>()
        var processedCount = 0

        cursor.forEach { groupedFlight ->
            val flightId = groupedFlight.getEmbedded(listOf("_id", "flightId"), Number::class.java)?.toInt()
            val track = groupedFlight.getEmbedded(listOf("_id", "track"), Number::class.java)?.toInt()
            val destination = groupedFlight.getEmbedded(listOf("_id", "destinationAirport"), String::class.java)
            val origin = groupedFlight.getEmbedded(listOf("_id", "originAirport"), String::class.java)
            val airplaneType = groupedFlight.getEmbedded(listOf("_id", "airplaneType"), String::class.java)
            val points = groupedFlight.getList("points", Document::class.java)
            val interpolatedPoints = mutableListOf<Document>()

            for (i in 0 until points.size - 1) {
                val currentPoint = points[i]
                val nextPoint = points[i + 1]
                interpolatePointsBetween(currentPoint, nextPoint, interpolatedPoints)
            }

            interpolatedPoints.forEach { point ->
                val metadata = Document("flightId", flightId).append("track", track).append("destinationAirport", destination)
                    .append("originAirport", origin).append("airplaneType", airplaneType)
                val timeSeriesDoc = Document("metadata", metadata)
                    .append("timestamp", point["timestamp"])
                    .append("altitude", point["altitude"])
                    .append("location", point["location"])
                bulkOperations.add(InsertOneModel(timeSeriesDoc))
            }

            if (bulkOperations.size >= batchSize) {
                flightPointsTsCollection.bulkWrite(bulkOperations, BulkWriteOptions().ordered(false))
                bulkOperations.clear()
                println("Executed bulk update for $batchSize documents (interpolation).")
            }

            processedCount++
        }

        if (bulkOperations.isNotEmpty()) {
            flightPointsTsCollection.bulkWrite(bulkOperations)
            println("Executed bulk update for remaining ${bulkOperations.size} documents (interpolation).")
        }

        println("Processed $processedCount groups of flightId and track (interpolation).")
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

    fun createTimeSeriesCollectionIndexes(){

        val flightPointsTsCollection = database.getCollection("flightpoints_ts")

        println("Creating more indexes in ${flightPointsTsCollection.namespace.collectionName}.")
        flightPointsTsCollection.createIndex(Document("originAirport", "hashed"))
        flightPointsTsCollection.createIndex(Document("destinationAirport", "hashed"))
        flightPointsTsCollection.createIndex(Document("location", "2dsphere"))

        var compoundIndex = Indexes.compoundIndex(
            Indexes.ascending("timestamp"), // Ascending index on timestamp
            Indexes.geo2dsphere("location"), // Geospatial index
        )

        var indexOptions = IndexOptions()
        flightPointsTsCollection.createIndex(compoundIndex, indexOptions)

        flightPointsTsCollection.createIndex(
            Document("metadata.flightId", 1).append("metadata.track", 1),
            indexOptions
        )


    }
}



fun main() {

    val separators = listOf(0, 700642631, 710076001, 718926541, 728177911, 736845861, 743346091, 754447851, 760302441, 773383481, 774441640)
    val handler = DataHandler("aviation_data")
    handler.updateDatabaseCollections()
    handler.shardCollections()
    handler.insertRegionalData()
    handler.createFlightTrips()
    handler.createTrajectories(separators)
    handler.createFlightsPointsTs(separators)
    handler.createTimeSeriesCollectionIndexes()

}


//private fun interpolatePointsTsBetween(currentPoint: Document, nextPoint: Document, result: MutableList<Document>) {
//    val currentTimestamp = currentPoint.getDate("timestamp").toInstant()
//    val nextTimestamp = nextPoint.getDate("timestamp").toInstant()
//
//    val currentLocation = currentPoint.get("location", Document::class.java)
//    val nextLocation = nextPoint.get("location", Document::class.java)
//
//    val currentCoordinatesList = currentLocation["coordinates"] as List<*>
//    val nextCoordinatesList = nextLocation["coordinates"] as List<*>
//
//    val currentLongitude = (currentCoordinatesList[0] as Number).toDouble()
//    val currentLatitude = (currentCoordinatesList[1] as Number).toDouble()
//    val nextLongitude = (nextCoordinatesList[0] as Number).toDouble()
//    val nextLatitude = (nextCoordinatesList[1] as Number).toDouble()
//
//    val currentAltitude = currentPoint.getDouble("altitude")
//    val nextAltitude = nextPoint.getDouble("altitude")
//
//    var interpolatedTimestamp = currentTimestamp.plusSeconds(1)
//
//    result.add(currentPoint)
//
//    while (interpolatedTimestamp.isBefore(nextTimestamp)) {
//        val currentSeconds = currentTimestamp.epochSecond
//        val nextSeconds = nextTimestamp.epochSecond
//        val interpolatedSeconds = interpolatedTimestamp.epochSecond
//
//        val interpolatedLongitude = currentLongitude + (nextLongitude - currentLongitude) * (interpolatedSeconds - currentSeconds) / (nextSeconds - currentSeconds)
//        val interpolatedLatitude = currentLatitude + (nextLatitude - currentLatitude) * (interpolatedSeconds - currentSeconds) / (nextSeconds - currentSeconds)
//        val interpolatedAltitude = currentAltitude + (nextAltitude - currentAltitude) * (interpolatedSeconds - currentSeconds) / (nextSeconds - currentSeconds)
//
//        result.add(Document().apply {
//            append("timestamp", java.util.Date.from(interpolatedTimestamp))
//            append("location", Document("type", "Point").append("coordinates", listOf(interpolatedLongitude, interpolatedLatitude)))
//            append("altitude", interpolatedAltitude)
//        })
//        interpolatedTimestamp = interpolatedTimestamp.plusSeconds(1)
//    }
//}

//suspend fun processFlightTrips(
//    flightTripsCollection: MongoCollection<Document>,
//    flightPointsTsCollection: MongoCollection<Document>,
//    batchSize: Int,
//    flightIdThreshold: Int,
//    isLarger: Boolean
//) {
//    val filter = if (isLarger) {
//        Document("flightId", Document("\$gt", flightIdThreshold))
//    } else {
//        Document("flightId", Document("\$lte", flightIdThreshold))
//    }
//
//    val bulkOps = mutableListOf<InsertOneModel<Document>>()
//
//    flightTripsCollection.find(filter).batchSize(batchSize).forEach { doc ->
//        val flightId = doc.getInteger("flightId")
//        val airplaneType = doc.getString("airplaneType")
//        val destinationAirport = doc.getString("destinationAirport")
//        val originAirport = doc.getString("originAirport")
//        val track = doc.getInteger("track")
//        val points = doc.getList("points", Document::class.java)
//
//        val metadata = Document(
//            mapOf(
//                "flightId" to flightId,
//                "track" to track,
//                "originAirport" to originAirport,
//                "destinationAirport" to destinationAirport,
//                "airplaneType" to airplaneType,
//            )
//        )
//
//        val currentBatch = points.map { point ->
//            InsertOneModel(
//                Document(
//                    mapOf(
//                        "metadata" to metadata,
//                        "timestamp" to point["timestamp"],
//                        "lon" to point["lon"],
//                        "lat" to point["lat"],
//                        "altitude" to point["altitude"],
//                    )
//                )
//            )
//        }
//
//        bulkOps.addAll(currentBatch)
//
//        if (bulkOps.size >= batchSize) {
//            flightPointsTsCollection.bulkWrite(bulkOps)
//            bulkOps.clear()
//        }
//    }
//
//    if (bulkOps.isNotEmpty()) {
//        flightPointsTsCollection.bulkWrite(bulkOps)
//    }
//
//}


//                .append("key", Document("metadata.flightId", 1).append("timestamp", 1))

// Define the aggregation pipeline
//            val pipeline = listOf(
//                Document(
//                    "\$project", Document(
//                        mapOf(
//                            "metadata" to Document(
//                                mapOf(
//                                    "flightId" to "\$flightId",
//                                    "destinationAirport" to "\$destinationAirport",
//                                    "originAirport" to "\$originAirport",
//                                    "track" to "\$track"
//                                )
//                            ),
//                            "timestamp" to "\$timestamp",
//                            "altitude" to "\$altitude",
//                            "location" to "\$location"
//                        )
//                    )
//                ),
//                Document(
//                    "\$out", "flightpoints_ts"
//                )
//            )


