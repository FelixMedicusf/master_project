import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
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
import org.bson.Document
import org.bson.conversions.Bson
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.impl.CoordinateArraySequence
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.*
import kotlin.collections.ArrayList
import kotlin.reflect.KFunction
import kotlin.reflect.full.declaredFunctions


// This class is used to prepare the MongoDB for the benchmark.
// Updating collections, inserting data about regions, creating the collection and inserting data for flightTrips
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
        val flightTripsCollection = database.getCollection("flighttrips")
        val flightPointsCollection = database.getCollection("flightpoints")
        println("Creating flighttrips ...")
        createTripsCollection(flightPointsCollection, flightTripsCollection)
        println("Interpolating flighttrips ...")
        interpolateFlightPoints(flightTripsCollection)
        println("Creating trajectories for flighttrips ...")
        createFlightTrajectories(flightTripsCollection)
    }

    fun shardCollections(){

        val flightTrips = database.getCollection("flighttrips")
        createHashedIndexAndShard(flightTrips, "flightId")

        for (region in regions){
            val collection = database.getCollection(region)
            createHashedIndexAndShard(collection, "name")
        }
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
            Document("\$sort", Document("timestamp", 1)), // Ensure points are sorted by timestamp
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
                append("timeRange", Document().apply {
                    append("start", Document("\$arrayElemAt", listOf("\$points.timestamp", 0))) // First timestamp
                    append("end", Document("\$arrayElemAt", listOf("\$points.timestamp", -1))) // Last timestamp
                })
            }),
            Document("\$project", Document().apply {
                append("_id", 0)
                append("flightId", 1)
                append("originAirport", 1)
                append("destinationAirport", 1)
                append("airplaneType", 1)
                append("track", 1)
                append("points", 1)
                append("timeRange", 1)
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


    private fun interpolateFlightPoints(collection: MongoCollection<Document>, batchSize: Int = 7500) {
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
                //println("Executed bulk update for $batchSize documents.")
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

    private fun createFlightTrajectories(collection: MongoCollection<Document>, batchSize: Int = 7500) {
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

    private fun createHashedIndexAndShard(collection: MongoCollection<Document>, columnName: String) {
        val collectionName = collection.namespace.collectionName
        val databaseName = collection.namespace.databaseName

        // Step 1: Create a hashed index on the specified column
        println("Creating hashed index on column '$columnName' in collection '$collectionName'...")
        collection.createIndex(Document(columnName, "hashed"))
        println("Hashed index created on column '$columnName'.")

        // Step 2: Shard the collection on the specified column
        println("Sharding collection '$collectionName' on column '$columnName' with hashed key...")
        adminDatabase.runCommand(
            Document("shardCollection", "$databaseName.$collectionName")
                .append("key", Document(columnName, "hashed"))
        )
        println("Collection '$collectionName' sharded on column '$columnName'.")
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
        val flightPointsCollectionName = "flightpoints"

        try {
//            database.getCollection(flightPointsCollectionName).drop()
//            println("Collection '$flightPointsCollectionName' deleted.")

            val timeSeriesOptions = TimeSeriesOptions("timestamp").metaField("flightId").granularity(TimeSeriesGranularity.SECONDS)
            val createOptions = CreateCollectionOptions()
                .timeSeriesOptions(timeSeriesOptions)
            database.createCollection(flightPointsTsCollection.namespace.collectionName, createOptions)

            flightPointsTsCollection.createIndex(Document("flightId", "hashed"))

            adminDatabase.runCommand(
                Document("shardCollection", "aviation_data.flightpoints_ts") // Namespace
                    .append("key", Document("flightId", "hashed")) // Correctly formatted key as a Document
            )

            println("Collection '${flightPointsTsCollection.namespace.collectionName}' created with time-series settings.")
        } catch (e: Exception) {
            println("Error while resetting the collection: ${e.message}")
            return
        }

        val bulkOps = mutableListOf<WriteModel<Document>>()
        val batchSize = 500000 // Adjust based on memory and dataset size

        try {
            flightTripsCollection.find().forEach { doc ->
                val flightId = doc.getInteger("flightId") // Metadata
                val points = doc.getList("points", Document::class.java) // Time-series data

                points.forEach { point ->
                    val bulkOp = InsertOneModel(
                        Document(mapOf(
                            "flightId" to flightId, // MetaField
                            "timestamp" to point["timestamp"], // TimeField
                            "location" to point["location"], // Additional fields
                            "altitude" to point["altitude"] // Additional fields
                        ))
                    )
                    bulkOps.add(bulkOp)

                    // Execute bulk write when batch size is reached
                    if (bulkOps.size >= batchSize) {
                        flightPointsTsCollection.bulkWrite(bulkOps)
                        bulkOps.clear() // Clear the bulkOps list
                    }
                }
            }

            // Execute any remaining operations
            if (bulkOps.isNotEmpty()) {
                flightPointsTsCollection.bulkWrite(bulkOps)
            }

            println("Time-series data migrated successfully!")
        } catch (e: Exception) {
            println("Error during migration: ${e.message}")
        }
    }


    private fun invokeFunctionByName(functionName: String): KFunction<*>? {
        val benchThreadClass = this::class

        val function = benchThreadClass.declaredFunctions.find { it.name == functionName }

        if (function != null) {
            return function
        } else {
            println("Function '$functionName' not found.")
            return benchThreadClass.declaredFunctions.find { it.name == "queryFlightsByPeriod" }
        }
    }

    private fun printMongoResponse(response: Any?) {
        // Configure Jackson ObjectMapper for pretty JSON formatting
        val mapper = ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)

        var castedResponse  = response as List<Document>
        println("MongoDB Response:")
        castedResponse.forEach { document ->
            try {
                // Convert the Document to a JSON string and print it
                val jsonString = mapper.writeValueAsString(document.toMap())
                println(jsonString)
            } catch (e: Exception) {
                println("Error formatting document: ${document.toJson()}")
            }
        }
    }

    fun runQueries() {
        val flightTripsCollection = database.getCollection("flighttrips")
        val flightPointsTsCollection = database.getCollection("flightpoints_ts")
        val airportsCollection = database.getCollection("airports")
        val citiesCollection = database.getCollection("cities")
        val municipalitiesCollection = database.getCollection("municipalities")
        val countiesCollection = database.getCollection("counties")
        val districtsCollection = database.getCollection("districts")

        val response3 = flightsInCountyDuringPeriod(countiesCollection, flightPointsTsCollection, "2023-01-01 12:00:00", "2023-01-10 12:00:00", "Krs Duisburg")
        //println(response3.fourth)
        printMongoResponse(response3.fourth)

    }

    fun queryFlightsByPeriod(
        flightTripsCollection: MongoCollection<Document>, vararg period: String
    ): Pair<Long, List<Document>> {

        val startDate: Date = dateFormat.parse(period[0])
        val endDate: Date = dateFormat.parse(period[1])

        // MongoDB aggregation pipeline
        val pipeline = listOf(
            Document("\$match", Document("points.timestamp", Document("\$gte", startDate).append("\$lte", endDate))),
            Document(
                "\$project", Document("period", Document("\$concat", listOf(period[0], " - ", period[1])))
            ),
            Document(
                "\$group", Document("_id", "\$period").append("count", Document("\$sum", 1))
            ),
            Document("\$sort", Document("_id", 1)) // Sort by period
        )


        val queryTimeStart = Instant.now().toEpochMilli()
        // Execute the aggregation
        val results = flightTripsCollection.aggregate(pipeline)

        // Convert results to a list of Documents
        return Pair(queryTimeStart, results.into(mutableListOf()))

    }

    fun locationOfAirplaneTypeAtInstant(
        flightTripsCollection: MongoCollection<Document>,
        instant: String
    ): Pair<Long, List<Document>> {

        val timestamp: Date = dateFormat.parse(instant)
        val pipeline = listOf(
            Document(
                "\$match", Document("points.timestamp", timestamp) // Filter documents with exact timestamp
            ),
            Document(
                "\$project", Document(
                    "flightid", 1) // Include flight ID
                    .append("time", instant) // Add the provided instant as a field
                    .append(
                        "altitudeAtTime", Document(
                            "\$arrayElemAt", listOf(
                                "\$points.altitude",
                                Document("\$indexOfArray", listOf("\$points.timestamp", timestamp))
                            )
                        )
                    )
                    .append(
                        "longitudeAtTime", Document(
                            "\$arrayElemAt", listOf(
                                Document("\$arrayElemAt", listOf("\$points.location.coordinates",
                                    Document("\$indexOfArray", listOf("\$points.timestamp", timestamp))
                                )), 0
                            )
                        )
                    )
                    .append(
                        "latitudeAtTime", Document(
                            "\$arrayElemAt", listOf(
                                Document("\$arrayElemAt", listOf("\$points.location.coordinates",
                                    Document("\$indexOfArray", listOf("\$points.timestamp", timestamp))
                                )), 1
                            )
                        )
                    )
            ),
            Document(
                "\$match", Document("longitudeAtTime", Document("\$ne", null)) // Ensure longitude is not null
            ),
            Document(
                "\$project", Document(
                    "flightid", 1
                ).append("time", 1)
                    .append("altitude", "\$altitudeAtTime")
                    .append("location", Document("\$concat", listOf(
                        "POINT(",
                        Document("\$toString", "\$longitudeAtTime"), " ",
                        Document("\$toString", "\$latitudeAtTime"), ")"
                    ))) // Convert coordinates to WKT POINT format
            )
        )

        val queryTimeStart = Instant.now().toEpochMilli()
        // Execute the aggregation
        val results = flightTripsCollection.aggregate(pipeline)

        // Convert results to a list of Documents
        return Pair(queryTimeStart, results.into(mutableListOf()))
    }

    fun locationOfLowFlyingAirplanesTypeAtInstant(
        flightpointsCollectionTs: MongoCollection<Document>,
        instant: String
    ): Pair<Long, List<Document>> {

        val timestamp: Date = dateFormat.parse(instant)

        val pipeline = listOf(

            Document("\$match", Document().apply {
                append("timestamp", timestamp)
                append("altitude", Document("\$lt", 5000))
            }),

            Document("\$project", Document().apply {
                append("flightId", 1)
                append("location", 1)
                append("_id", 0)
            })
        )

        val queryTimeStart = Instant.now().toEpochMilli()
        // Execute the aggregation
        return Pair(queryTimeStart, flightpointsCollectionTs.aggregate(pipeline).into(mutableListOf()))
    }

    fun flightTimeLowAltitude(
        flightTripsCollection: MongoCollection<Document>,
        startPeriod: String,
        endPeriod: String
    ): Pair<Long, List<Document>> {

        val startDate: Date = dateFormat.parse(startPeriod)
        val endDate: Date = dateFormat.parse(endPeriod)

        val pipeline = listOf(

            Document(
                "\$match", Document(
                    "points", Document(
                        "\$elemMatch", Document(
                            "timestamp", Document("\$gte", startDate).append("\$lte", endDate)
                        )
                    )
                )
            ),

            Document(
                "\$match", Document(
                    "points", Document(
                        "\$elemMatch", Document(
                            "altitude", Document("\$lt", 4000)
                        )
                    )
                )
            ),

            Document(
                "\$project", Document(
                    "flightId", 1
                ).append(
                    "durations", Document(
                        "\$map", Document(
                            "input", Document(
                                "\$range", listOf(0, Document("\$subtract", listOf(Document("\$size", "\$points"), 1)))
                            )
                        ).append(
                            "as", "index"
                        ).append(
                            "in", Document(
                                "altitude", Document("\$arrayElemAt", listOf("\$points.altitude", "\$\$index"))
                            ).append(
                                "startTime", Document("\$arrayElemAt", listOf("\$points.timestamp", "\$\$index"))
                            ).append(
                                "endTime", Document("\$arrayElemAt", listOf("\$points.timestamp", Document("\$add", listOf("\$\$index", 1))))
                            )
                        )
                    )
                )
            ),

            Document(
                "\$project", Document(
                    "flightId", 1
                ).append(
                    "totalTime", Document(
                        "\$sum", Document(
                            "\$map", Document(
                                "input", "\$durations"
                            ).append(
                                "as", "duration"
                            ).append(
                                "in", Document(
                                    "\$divide", listOf(
                                        Document("\$subtract", listOf("\$\$duration.endTime", "\$\$duration.startTime")),
                                        1000
                                    )
                                )
                            )
                        )
                    )
                ).append(
                    "timeBelow4000", Document(
                        "\$sum", Document(
                            "\$map", Document(
                                "input", "\$durations"
                            ).append(
                                "as", "duration"
                            ).append(
                                "in", Document(
                                    "\$cond", listOf(
                                        Document("\$lt", listOf("\$\$duration.altitude", 4000)),
                                        Document(
                                            "\$divide", listOf(
                                                Document("\$subtract", listOf("\$\$duration.endTime", "\$\$duration.startTime")),
                                                1000
                                            )
                                        ),
                                        0
                                    )
                                )
                            )
                        )
                    )
                )
            ),

            Document(
                "\$addFields", Document(
                    "ratio", Document(
                        "\$cond", listOf(
                            Document("\$gt", listOf("\$totalTime", 0)),
                            Document("\$divide", listOf("\$timeBelow4000", "\$totalTime")),
                            0
                        )
                    )
                )
            ),

            Document(
                "\$project", Document(
                    "_id", 0
                ).append(
                    "flightId", 1
                ).append(
                    "timeBelow4000", 1
                ).append(
                    "totalTime", 1
                ).append(
                    "ratio", 1
                )
            )
        )

        val queryTimeStart = Instant.now().toEpochMilli()

        return Pair(queryTimeStart, flightTripsCollection.aggregate(pipeline).allowDiskUse(true).toList())
    }

    fun averageHourlyFlightsDuringDay(
        flightPointsTsCollection: MongoCollection<Document>,
        dateString: String
    ): Pair<Long, List<Document>> {
        val date = LocalDate.parse(dateString.trim('\'')) // Trim single quotes and parse

        val startOfDay = Date.from(date.atStartOfDay().toInstant(ZoneOffset.UTC))
        val endOfDay = Date.from(date.plusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC))

        // List of all hours (0-23)
        val hours = (0..23).toList()

        val pipeline = listOf(
            // Step 1: Filter documents for the specified day
            Document("\$match", Document("timestamp", Document("\$gte", startOfDay).append("\$lt", endOfDay))),

            // Step 2: Add a field for the hour of the timestamp
            Document("\$addFields", Document("hour", Document("\$hour", "\$timestamp"))),

            // Step 3: Group by hour and collect distinct flightIds
            Document("\$group", Document().apply {
                append("_id", "\$hour") // Group by hour
                append("distinctFlights", Document("\$addToSet", "\$flightId")) // Collect distinct flightIds
            }),

            // Step 4: Calculate the number of flights per hour
            Document("\$project", Document().apply {
                append("_id", 0) // Exclude MongoDB's default _id field
                append("hour", "\$_id") // Rename the _id field to hour
                append("flightCount", Document("\$size", "\$distinctFlights")) // Count the distinct flights
            }),

            // Step 5: Sort by hour
            Document("\$sort", Document("hour", 1))
        )


        val queryTimeStart = Instant.now().toEpochMilli()

        return Pair(queryTimeStart, flightPointsTsCollection.aggregate(pipeline).toList())
    }


    fun flightsWithLocalOriginDestinationDuringPeriod(
        airportsCollection: MongoCollection<Document>,
        periodStart: String,
        periodEnd: String
    ): Pair<Long, List<Document>>{

        val startDate: Date = dateFormat.parse(periodStart)
        val endDate: Date = dateFormat.parse(periodEnd)

        val pipeline = listOf(
            // Stage 1: Lookup cities for airports
            Document(
                "\$lookup", Document("from", "cities")
                    .append("localField", "City")
                    .append("foreignField", "name")
                    .append("as", "cityDetails")
            ),
            Document("\$match", Document("cityDetails", Document("\$ne", emptyList<Any>()))),
            Document("\$project", Document("ICAO", 1)
                .append("cityName", Document("\$arrayElemAt", listOf("\$cityDetails.name", 0)))
                .append("_id", 0)),

            // Stage 2: Group local airports into a list
            Document("\$group", Document("_id", null)
                .append("localAirports", Document("\$addToSet", "\$ICAO"))),

            // Stage 3: Pass localAirports list to the lookup
            Document("\$lookup", Document("from", "flighttrips")
                .append("let", Document("localAirports", "\$localAirports"))
                .append("pipeline", listOf(
                    // Match flighttrips with overlapping timeRange
                    Document(
                        "\$match", Document("\$expr", Document("\$or", listOf(
                            Document("\$and", listOf(
                                Document("\$lte", listOf("\$timeRange.start", endDate)),
                                Document("\$gte", listOf("\$timeRange.end", startDate))
                            )),
                            Document("\$and", listOf(
                                Document("\$lte", listOf("\$timeRange.start", startDate)),
                                Document("\$gte", listOf("\$timeRange.end", endDate))
                            ))
                        )))
                    ),
                    // Match flighttrips where origin or destination matches local airports
                    Document(
                        "\$match", Document("\$expr", Document("\$or", listOf(
                            Document("\$in", listOf("\$originAirport", "\$\$localAirports")),
                            Document("\$in", listOf("\$destinationAirport", "\$\$localAirports"))
                        )))
                    )
                ))
                .append("as", "filteredFlightTrips")
            ),
            Document("\$project", Document("filteredFlightTrips.points", 0).append("filteredFlightTrips.trajectory", 0).append("filteredFlightTrips.timeRange", 0).append("filteredFlightTrips.track", 0)),
            Document("\$unwind", "\$filteredFlightTrips"),
            Document(
                "\$lookup", Document("from", "airports")
                    .append("localField", "filteredFlightTrips.originAirport")
                    .append("foreignField", "ICAO")
                    .append("as", "cityNameOrigin")
            ),
            Document(
                "\$lookup", Document("from", "airports")
                    .append("localField", "filteredFlightTrips.destinationAirport")
                    .append("foreignField", "ICAO")
                    .append("as", "cityNameDestination")
            ),
            // Stage 4: Project final fields
            Document("\$project", Document()
                .append("_id", 0)
                .append("flightId", "\$filteredFlightTrips.flightId")
                .append("originAirport", "\$filteredFlightTrips.originAirport")
                .append("destinationAirport", "\$filteredFlightTrips.destinationAirport")
                .append("originCity", "\$cityNameOrigin.City")
                .append("destinationCity", "\$cityNameDestination.City")
                .append("airplaneType", "\$filteredFlightTrips.airplaneType")
       )
        )

        val queryTimeStart = Instant.now().toEpochMilli()

        return Pair(queryTimeStart, airportsCollection.aggregate(pipeline).toList())
    }

    fun airportUtilizationDuringPeriod(
        flightTripsCollection: MongoCollection<Document>,
        periodStart: String,
        periodEnd: String
    ): Pair<Long, List<Document>> {
        val startDate: Date = dateFormat.parse(periodStart)
        val endDate: Date = dateFormat.parse(periodEnd)

        val pipeline = listOf(
            // Stage 1: Filter flights based on timeRange overlap with the period
            Document(
                "\$match", Document("\$expr", Document("\$or", listOf(
                    Document("\$and", listOf(
                        Document("\$lte", listOf("\$timeRange.start", endDate)),
                        Document("\$gte", listOf("\$timeRange.end", startDate))
                    )),
                    Document("\$and", listOf(
                        Document("\$lte", listOf("\$timeRange.start", startDate)),
                        Document("\$gte", listOf("\$timeRange.end", endDate))
                    ))
                )))
            ),
            Document(
                "\$facet", Document("departures", listOf(
                    Document(
                        "\$group", Document("_id", "\$originAirport")
                            .append("departure_count", Document("\$sum", 1))
                    )
                ))
                    .append("arrivals", listOf(
                        Document(
                            "\$group", Document("_id", "\$destinationAirport")
                                .append("arrival_count", Document("\$sum", 1))
                        )
                    ))
            ),

            // Combine results
            Document(
                "\$project", Document("combined", Document("\$concatArrays", listOf("\$departures", "\$arrivals")))
            ),
            Document(
                "\$unwind", "\$combined"
            ),
            Document(
                "\$replaceRoot", Document("newRoot", "\$combined")
            ),
            Document(
                "\$group", Document("_id", "\$_id")
                    .append("departure_count", Document("\$sum", Document("\$ifNull", listOf("\$departure_count", 0))))
                    .append("arrival_count", Document("\$sum", Document("\$ifNull", listOf("\$arrival_count", 0))))
            ),
            // Add traffic_count (sum of arrivals and departures)
            Document(
                "\$addFields", Document("traffic_count", Document("\$add", listOf("\$departure_count", "\$arrival_count")))
            ),
            // Sort by traffic_count, departure_count, and arrival_count
            Document(
                "\$sort", Document("traffic_count", -1)
                    .append("departure_count", -1)
                    .append("arrival_count", -1)
            )
        )

        val queryTimeStart = Instant.now().toEpochMilli()
        val result = flightTripsCollection.aggregate(pipeline).toList()

        return Pair(queryTimeStart, result)
}

    fun flightsInCityRadius(
        citiesCollection: MongoCollection<Document>,
        flightTripsCollection: MongoCollection<Document>,
        cityName: String,
        radius: Int
    ):Quadrupel<Long, Long, Long, List<Document>> {


        val firstPipeline = listOf(
            Document("\$match", Document("name", cityName)),
            Document("\$project", Document("location.coordinates", 1).append("_id", 0).append("name", 1))
        )



        val queryTimeStartFirstQuery = Instant.now().toEpochMilli()

        val firstResponse = citiesCollection.aggregate(firstPipeline).toList()
        val queryEndTimeFirstQuery = Instant.now().toEpochMilli()

        val name = firstResponse.get(0).getString("name")
        val coordinates = firstResponse.flatMap { document ->
            val location = document.get("location", Document::class.java) // Get 'location' sub-document
            val coords = location?.get("coordinates", List::class.java) // Get 'coordinates' as a List
            coords?.map { it as Double } ?: emptyList() // Ensure null-safe mapping
        }

        val secondPipeline = listOf(
            Document("\$match", Document("points.location",
                Document("\$geoWithin",
                    Document("\$centerSphere", listOf(coordinates, radius))
                )
            )),
            Document("\$project", Document("flightId", 1).append("airplaneType", 1).append("_id", 0))
        )


        val queryTimeStartSecondQuery = Instant.now().toEpochMilli()
        val secondResponse = flightTripsCollection.aggregate(secondPipeline).toList()


        return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartSecondQuery, secondResponse)

    }

    fun flightsIntersectingMunicipalities(
        municipalitiesCollection: MongoCollection<Document>,
        flightTripsCollection: MongoCollection<Document>,
        municipalityName: String
    ):Quadrupel<Long, Long, Long, List<Document>> {


        val firstPipeline = listOf(
            Document("\$match", Document("name", municipalityName)),
            Document("\$project", Document("polygon.coordinates", 1).append("_id", 0).append("name", 1))
        )

        val queryTimeStartFirstQuery = Instant.now().toEpochMilli()

        val firstResponse = municipalitiesCollection.aggregate(firstPipeline).toList()

        val queryEndTimeFirstQuery = Instant.now().toEpochMilli()
        //println(firstResponse)
        val name = firstResponse.get(0).getString("name")
        val polygonCoordinates = firstResponse.mapNotNull { document ->
            val polygon = document.get("polygon", Document::class.java) // Get 'polygon' sub-document
            polygon?.get("coordinates", List::class.java) // Get 'coordinates' as a List
        }[0]

        val secondPipeline = listOf(
            Document("\$match", Document("trajectory",
                Document("\$geoIntersects",
                    Document("\$geometry", Document("type", "Polygon").append("coordinates", polygonCoordinates))
                )
            )),
            Document("\$project", Document("flightId", 1).append("airplaneType", 1).append("_id", 0).append("originAirport", 1).append("destinationAirport", 1))
        )


        val queryTimeStartSecondQuery = Instant.now().toEpochMilli()
        val secondResponse = flightTripsCollection.aggregate(secondPipeline).toList()


        return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartSecondQuery, secondResponse)

    }


    fun countFlightsInCounties(
        countiesCollection: MongoCollection<Document>,
        flightTripsCollection: MongoCollection<Document>,
        countyName: String
        ): Quadrupel<Long, Long, Long, List<Document>> {

        val firstPipeline = listOf(
            Document("\$match", Document("name", countyName)),
            Document("\$project", Document("polygon.coordinates", 1).append("_id", 0).append("name", 1))
        )

        val queryTimeStartFirstQuery = Instant.now().toEpochMilli()

        val firstResponse = countiesCollection.aggregate(firstPipeline).toList()

        val queryEndTimeFirstQuery = Instant.now().toEpochMilli()
        //println(firstResponse)
        val name = firstResponse.get(0).getString("name")
        val polygonCoordinates = firstResponse.mapNotNull { document ->
            val polygon = document.get("polygon", Document::class.java) // Get 'polygon' sub-document
            polygon?.get("coordinates", List::class.java) // Get 'coordinates' as a List
        }[0]

        val secondPipeline = listOf(
            Document(
                "\$match", Document(
                    "trajectory",
                    Document(
                        "\$geoIntersects",
                        Document("\$geometry", Document("type", "Polygon").append("coordinates", polygonCoordinates))
                    )
                )
            ),
            Document("\$project",
                Document("flightId", 1).append("airplaneType", 1).append("_id", 0).append("originAirport", 1)
                    .append("destinationAirport", 1)
            ),
            Document("\$count", "flight_count")
        )


        val queryTimeStartSecondQuery = Instant.now().toEpochMilli()
        val secondResponse = flightTripsCollection.aggregate(secondPipeline).toList()


        return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartSecondQuery, secondResponse)
    }

    fun flightsCloseToMainCitiesLowAltitude(
        citiesCollection: MongoCollection<Document>,
        flightPointsCollection: MongoCollection<Document>,
        lowAltitude: Int,
        radius: Int,
        population: Int
    ): Quadrupel<Long, Long, Long, List<Document>> {

        val firstPipeline = listOf(
            Document("\$match", Document("population", Document("\$gt", population))),
            Document("\$project", Document("coordinates", "\$location.coordinates").append("_id", 0).append("name", 1)),
            Document("\$group", Document("_id", null)
                .append("coordinatePairs", Document("\$push", "\$coordinates"))
                .append("names", Document("\$push", "\$name"))
            )
        )

        val queryTimeStartFirstQuery = Instant.now().toEpochMilli()

        val firstResponse = citiesCollection.aggregate(firstPipeline).toList()
        val queryEndTimeFirstQuery = Instant.now().toEpochMilli()

        if (firstResponse.isEmpty()) {
            println("No cities found matching the criteria.")
            return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartFirstQuery, emptyList())
        }

        val coordinatePairs = firstResponse.firstOrNull()?.get("coordinatePairs", List::class.java)
        val geoWithinConditions = coordinatePairs?.map { pair ->
            Document("location", Document("\$geoWithin", Document("\$centerSphere", listOf(pair, radius.toDouble() / 6378.1))))
        }

        if (geoWithinConditions.isNullOrEmpty()) {
            println("No coordinate pairs to create geoWithin conditions.")
            return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartFirstQuery, emptyList())
        }

        val secondPipeline = listOf(
            Document("\$match", Document("altitude", Document("\$lt", lowAltitude))),
            Document("\$match", Document("\$or", geoWithinConditions)), // Use $or for multiple geoWithin conditions
            Document("\$project", Document("flightId", 1).append("altitude", 1).append("airplaneType", 1).append("_id", 0).append("timestamp", 1)),
            Document("\$group", Document("_id", Document("flightId", "\$flightId")
                .append("altitude", "\$altitude")
                .append("airplaneType", "\$airplaneType")
                .append("timestamp", "\$timestamp")))
        )

        val queryTimeStartSecondQuery = Instant.now().toEpochMilli()
        val secondResponse = flightPointsCollection.aggregate(secondPipeline).toList()

        return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartSecondQuery, secondResponse)
    }

    fun flightsOnlyInOneDistrict(
        districtsCollection: MongoCollection<Document>,
        flightTripsCollection: MongoCollection<Document>,
        districtName: String
        ):Quadrupel<Long, Long, Long, List<Document>>{

        val firstPipeline = listOf(
            Document("\$match", Document("name", districtName)),
            Document("\$project", Document("polygon.coordinates", 1).append("_id", 0).append("name", 1))
        )

        val queryTimeStartFirstQuery = Instant.now().toEpochMilli()
        val firstResponse = districtsCollection.aggregate(firstPipeline).toList()
        val queryEndTimeFirstQuery = Instant.now().toEpochMilli()

        val name = firstResponse.get(0).getString("name")
        val polygonCoordinates = firstResponse.mapNotNull { document ->
            val polygon = document.get("polygon", Document::class.java) // Get 'polygon' sub-document
            polygon?.get("coordinates", List::class.java) // Get 'coordinates' as a List
        }[0]

        val secondPipeline = listOf(
            Document(
                "\$match", Document(
                    "trajectory",
                    Document(
                        "\$geoWithin",
                        Document("\$geometry", Document("type", "Polygon").append("coordinates", polygonCoordinates))
                    )
                )
            ),
            Document("\$project",
                Document("flightId", 1).append("airplaneType", 1).append("_id", 0).append("originAirport", 1)
                    .append("destinationAirport", 1)
            )
        )

        val queryTimeStartSecondQuery = Instant.now().toEpochMilli()
        val secondResponse = flightTripsCollection.aggregate(secondPipeline).toList()

        return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartSecondQuery, secondResponse)

    }

    fun countiesLandingsDepartures(
        countiesCollection: MongoCollection<Document>,
        citiesCollection: MongoCollection<Document>,
        flightTripsCollection: MongoCollection<Document>,
        countyName: String): Quadrupel<Long, Long, Long, List<Document>> {

        val firstPipeline = listOf(
            Document("\$match", Document("name", countyName)),
            Document("\$project", Document("polygon.coordinates", 1).append("_id", 0).append("name", 1))
        )

        val queryTimeStartFirstQuery = Instant.now().toEpochMilli()
        val firstResponse = countiesCollection.aggregate(firstPipeline).toList()
        val queryEndTimeFirstQuery = Instant.now().toEpochMilli()

        val name = firstResponse.get(0).getString("name")
        val polygonCoordinates = firstResponse.mapNotNull { document ->
            val polygon = document.get("polygon", Document::class.java)
            polygon?.get("coordinates", List::class.java)
        }[0]

        //println(polygonCoordinates)

        val secondPipeline = listOf(

            Document("\$match",
                Document("location",
                    Document("\$geoWithin",
                        Document("\$geometry",
                            Document("type", "Polygon").append("coordinates", polygonCoordinates)
                        )
                    )
                )
            ),
            Document("\$project", Document("name", 1)),

            Document("\$lookup", Document("from", "airports")
                .append("localField", "name")
                .append("foreignField", "City")
                .append("as", "airports_in_county")
            ),

            Document("\$unwind", "\$airports_in_county"),

            Document("\$group", Document("_id", null)
                .append("icaoCodes", Document("\$addToSet", "\$airports_in_county.ICAO"))
            ),

            Document("\$lookup", Document("from", "flighttrips")
                .append("let", Document("icaoCodes", "\$icaoCodes"))
                .append("pipeline", listOf(
                    Document("\$match", Document("\$expr", Document(
                        "\$or", listOf(
                            Document("\$in", listOf("\$originAirport", "\$\$icaoCodes")),
                            Document("\$in", listOf("\$destinationAirport", "\$\$icaoCodes"))
                        )
                    ))),
                    Document("\$project", Document("originAirport", 1).append("destinationAirport", 1))
                ))
                .append("as", "related_flights")
            ),

            // Step 8: Count arrivals and departures
            Document("\$unwind", "\$related_flights"),
            Document("\$group", Document("_id", null)
                .append("arrivals", Document("\$sum", Document("\$cond", listOf(
                    Document("\$in", listOf("\$related_flights.destinationAirport", "\$icaoCodes")), 1, 0
                ))))
                .append("departures", Document("\$sum", Document("\$cond", listOf(
                    Document("\$in", listOf("\$related_flights.originAirport", "\$icaoCodes")), 1, 0
                ))))
            ),
            // Step 9: Calculate overall traffic
            Document("\$addFields", Document("overall_traffic", Document("\$add", listOf("\$arrivals", "\$departures")))),
            Document("\$project", Document("_id", 0))
        )
        val queryTimeStartSecondQuery = Instant.now().toEpochMilli()
        val response = citiesCollection.aggregate(secondPipeline).toList()

        return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartSecondQuery, response)

    }

    /*
    fun flightDistancesInMunicipalities(
        municipalitiesCollection: MongoCollection<Document>,
        ): Long {

    }

     */

    fun flightsInCountyDuringPeriod(
        countiesCollection: MongoCollection<Document>,
        flightPointsTsCollection: MongoCollection<Document>,
        startPeriod: String,
        endPeriod: String,
        countyName: String
    ): Quadrupel<Long, Long, Long, List<Document>>{


        val startDate: Date = dateFormat.parse(startPeriod)
        val endDate: Date = dateFormat.parse(endPeriod)

        val firstPipeline = listOf(
            Document("\$match", Document("name", countyName)),
            Document("\$project", Document("polygon.coordinates", 1).append("_id", 0).append("name", 1))
        )

        val queryTimeStartFirstQuery = Instant.now().toEpochMilli()
        val firstResponse = countiesCollection.aggregate(firstPipeline).toList()
        val queryEndTimeFirstQuery = Instant.now().toEpochMilli()

        val name = firstResponse.get(0).getString("name")
        val polygonCoordinates = firstResponse.mapNotNull { document ->
            val polygon = document.get("polygon", Document::class.java)
            polygon?.get("coordinates", List::class.java)
        }[0]

        val secondPipeline = listOf(
            Document(
                "\$match", Document(
                    "\$and", listOf(
                        Document(
                            "location", Document(
                                "\$geoWithin", Document(
                                    "\$geometry", Document("type", "Polygon").append("coordinates", polygonCoordinates)
                                )
                            )
                        ),

                    )
                )
            ),
            Document("\$group", Document("_id", "\$flightId")
            ),

            )

        val queryTimeStartSecondQuery = Instant.now().toEpochMilli()
        val response = flightPointsTsCollection.aggregate(secondPipeline).toList()

        return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartSecondQuery, response)



    }


}




fun main() {
    val handler = DataHandler("aviation_data")
//    handler.updateDatabaseCollections()
//    handler.shardCollections()
//    handler.insertRegionalData()
//    handler.createFlightTrips()
//    handler.createTimeSeriesCollection()
     handler.runQueries()
}
