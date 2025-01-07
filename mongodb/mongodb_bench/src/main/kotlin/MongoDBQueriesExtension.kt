import com.mongodb.client.MongoCollection
import org.bson.Document
import java.text.SimpleDateFormat
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.*

private val dateFormat = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

// temporal queries
fun BenchThread.countActiveFlightsInPeriod(
    staticCollections: List<MongoCollection<Document>>,
    dynamicCollections: List<MongoCollection<Document>>,
    period: List<String>,
    ): Quadrupel<Long, Long, Long, List<Document>> {

    val flightTripsCollection = dynamicCollections[2]

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
    return Quadrupel(queryTimeStart, 0, 0, results.into(mutableListOf()))

}

fun BenchThread.locationOfAirplaneAtInstant(
    staticCollections: List<MongoCollection<Document>>,
    dynamicCollections: List<MongoCollection<Document>>,
    instant: String
): Quadrupel<Long, Long, Long, List<Document>> {

    val flightTripsCollection = dynamicCollections[2]

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
                )))
        )
    )

    val queryTimeStart = Instant.now().toEpochMilli()
    // Execute the aggregation
    val results = flightTripsCollection.aggregate(pipeline)

    // Convert results to a list of Documents
    return Quadrupel(queryTimeStart, 0, 0, results.into(mutableListOf()))
}

fun BenchThread.flightTimeLowAltitude(
    staticCollections: List<MongoCollection<Document>>,
    dynamicCollections: List<MongoCollection<Document>>,
    period: List<String>
): Quadrupel<Long, Long, Long, List<Document>> {

    val flightTripsCollection = dynamicCollections[2]

    val startDate: Date = dateFormat.parse(period[0])
    val endDate: Date = dateFormat.parse(period[1])

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

    return Quadrupel(queryTimeStart, 0, 0, flightTripsCollection.aggregate(pipeline).allowDiskUse(true).toList())
}

fun BenchThread.averageHourlyFlightsDuringDay(
    staticCollections: List<MongoCollection<Document>>,
    dynamicCollections: List<MongoCollection<Document>>,
    day: String
): Quadrupel<Long, Long, Long, List<Document>> {

    val flightPointsTsCollection = dynamicCollections[1]

    val date = LocalDate.parse(day.trim('\'')) // Trim single quotes and parse

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

    return Quadrupel(queryTimeStart, 0, 0, flightPointsTsCollection.aggregate(pipeline).toList())
}

fun BenchThread.flightsWithLocalOriginDestinationInPeriod(
    staticCollections: List<MongoCollection<Document>>,
    dynamicCollections: List<MongoCollection<Document>>,
    period: List<String>
): Quadrupel<Long, Long, Long, List<Document>>{

    val airportsCollection = staticCollections[1]

    val startDate: Date = dateFormat.parse(period[0])
    val endDate: Date = dateFormat.parse(period[1])

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

    return Quadrupel(queryTimeStart, 0, 0, airportsCollection.aggregate(pipeline).toList())
}

fun BenchThread.airportUtilizationInPeriod(
    staticCollections: List<MongoCollection<Document>>,
    dynamicCollections: List<MongoCollection<Document>>,
    period:List<String>
): Quadrupel<Long, Long, Long, List<Document>> {

    val flightTripsCollection = dynamicCollections[2]

    val startDate: Date = dateFormat.parse(period[0])
    val endDate: Date = dateFormat.parse(period[1])

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

    return Quadrupel(queryTimeStart, 0, 0, result)
}



// spatial queries
fun BenchThread.flightsInCityRadius(
    staticCollections: List<MongoCollection<Document>>,
    dynamicCollections: List<MongoCollection<Document>>,
    cityName: String,
    radius: Int
):Quadrupel<Long, Long, Long, List<Document>> {

    val citiesCollection = staticCollections[0]
    val flightTripsCollection = dynamicCollections[2]

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

fun BenchThread.flightsIntersectingMunicipalities(
    staticCollections: List<MongoCollection<Document>>,
    dynamicCollections: List<MongoCollection<Document>>,
    municipalityName: String
):Quadrupel<Long, Long, Long, List<Document>> {

    val municipalitiesCollection = staticCollections[2]
    val flightTripsCollection = dynamicCollections[2]

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

fun BenchThread.countFlightsInCounties(
    staticCollections: List<MongoCollection<Document>>,
    dynamicCollections: List<MongoCollection<Document>>,
    countyName: String
): Quadrupel<Long, Long, Long, List<Document>> {

    val countiesCollection = staticCollections[3]
    val flightTripsCollection = dynamicCollections[2]

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

fun BenchThread.flightsCloseToMainCitiesLowAltitude(
    staticCollections: List<MongoCollection<Document>>,
    dynamicCollections: List<MongoCollection<Document>>,
    low_altitude: Int,
    radius: Int,
    population: Int
): Quadrupel<Long, Long, Long, List<Document>> {

    val citiesCollection = staticCollections[0]
    val flightPointsCollection = dynamicCollections[0]

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
        Document("\$match", Document("altitude", Document("\$lt", low_altitude))),
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

fun BenchThread.flightsOnlyInOneDistrict(
    staticCollections: List<MongoCollection<Document>>,
    dynamicCollections: List<MongoCollection<Document>>,
    districtName: String
):Quadrupel<Long, Long, Long, List<Document>>{

    val districtsCollection = staticCollections[4]
    val flightTripsCollection = dynamicCollections[2]

    val firstPipeline = listOf(
        Document("\$match", Document("name", districtName)),
        Document("\$project", Document("polygon.coordinates", 1).append("_id", 0).append("name", 1))
    )

    val queryTimeStartFirstQuery = Instant.now().toEpochMilli()

    val firstResponse = districtsCollection.aggregate(firstPipeline).toList()

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
    println(secondResponse.size)

    return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartSecondQuery, secondResponse)




}

fun BenchThread.countiesLandingsDepartures(
    staticCollections: List<MongoCollection<Document>>,
    dynamicCollections: List<MongoCollection<Document>>,
    countyName: String): Quadrupel<Long, Long, Long, List<Document>> {

    val countiesCollection = staticCollections[3]
    val citiesCollection = staticCollections[0]

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

fun BenchThread.flightClosestToPoint(
    staticCollections: List<MongoCollection<Document>>,
    dynamicCollections: List<MongoCollection<Document>>,
    coordinates: List<Double>,
    maxDistance: Int,
): Quadrupel<Long, Long, Long, List<Document>>{

    val flightTripsCollection = dynamicCollections[2]

    val pipeline = listOf(
        Document("\$geoNear", Document()
            .append("near", Document("type", "Point").append("coordinates", coordinates))
            .append("distanceField", "dist.calculated")
            .append("maxDistance", maxDistance)
            .append("key", "trajectory")
            .append("includeLocs", "dist.location")
            .append("spherical", true)
        ),
        Document("\$project", Document()
            .append("airplaneType", 1)
            .append("originAirport", 1)
            .append("calculatedDistance", "\$dist.calculated")
            .append("flightId", 1)
            .append("_id", 0)
        )
    )

    val queryStartTime = Instant.now().toEpochMilli()
    val response = flightTripsCollection.aggregate(pipeline).toList()

    return Quadrupel(queryStartTime, 0, 0, response)
}



// spatiotemporal queries
fun BenchThread.flightsInCountyInPeriod(
    staticCollections: List<MongoCollection<Document>>,
    dynamicCollections: List<MongoCollection<Document>>,
    period: List<String>,
    countyName: String
): Quadrupel<Long, Long, Long, List<Document>>{

    val countiesCollection = staticCollections[3]
    val flightPointsTsCollection = dynamicCollections[1]

    val startDate: Date = dateFormat.parse(period[0])
    val endDate: Date = dateFormat.parse(period[1])

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

    var secondPipeline = listOf(
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
                    Document(
                        "timestamp", Document(
                            "\$gte", startDate
                        ).append(
                            "\$lte", endDate
                        )
                    )
                )
            )
        ),
        Document("\$group", Document("_id", "\$flightId"))
    )

    secondPipeline = listOf(
        Document("\$unwind", "\$points"), // Unwind the points array
        Document(
            "\$match", Document(
                "\$and", listOf(
                    Document(
                        "points.location", Document( // Access the location field inside points
                            "\$geoWithin", Document(
                                "\$geometry", Document("type", "Polygon").append("coordinates", polygonCoordinates)
                            )
                        )
                    ),
                    Document(
                        "points.timestamp", Document( // Access the timestamp field inside points
                            "\$gte", startDate
                        ).append(
                            "\$lte", endDate
                        )
                    )
                )
            )
        ),
        Document("\$group", Document("_id", "\$flightId")) // Group by flightId
    )

    val queryTimeStartSecondQuery = Instant.now().toEpochMilli()
    val response = flightPointsTsCollection.aggregate(secondPipeline).toList()

    return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartSecondQuery, response)

}

fun BenchThread.pairOfFlightsInMunicipalityInPeriod(
    staticCollections: List<MongoCollection<Document>>,
    dynamicCollections: List<MongoCollection<Document>>,
    period: List<String>,
    municipalityName: String
): Quadrupel<Long, Long, Long, List<Document>>{

    val municipalitiesCollection = staticCollections[2]
    val flightTripsCollection = dynamicCollections[2]

    val startDate: Date = dateFormat.parse(period[0])
    val endDate: Date = dateFormat.parse(period[1])

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
        // Step 1: Unwind the points array to access individual timestamps and locations
        Document("\$unwind", "\$points"),

        // Step 2: Match flights where `points.location` is within the polygon and the `points.timestamp` is within the period
        Document(
            "\$match", Document(
                "\$and", listOf(
                    Document(
                        "points.location", Document(
                            "\$geoWithin", Document(
                                "\$geometry", Document("type", "Polygon").append("coordinates", polygonCoordinates)
                            )
                        )
                    ),
                    Document(
                        "points.timestamp", Document(
                            "\$gte", startDate // Start of the period
                        ).append(
                            "\$lte", endDate // End of the period
                        )
                    )
                )
            )
        ),

        // Step 3: Perform the self-join to find flight pairs
        Document(
            "\$lookup", Document()
                .append("from", "flighttrips")
                .append(
                    "let", Document()
                        .append("f1_flightId", "\$flightId")
                        .append("f1_points", "\$points")
                )
                .append(
                    "pipeline", listOf(
                        Document("\$unwind", "\$points"),
                        Document(
                            "\$match", Document(
                                "\$and", listOf(
                                    Document("\$expr", Document("\$lt", listOf("\$flightId", "\$\$f1_flightId"))), // f2.flightId < f1.flightId
                                    Document(
                                        "points.location", Document(
                                            "\$geoWithin", Document(
                                                "\$geometry", Document("type", "Polygon").append("coordinates", polygonCoordinates)
                                            )
                                        )
                                    ),
                                    Document(
                                        "\$expr", Document("\$eq", listOf("\$points.timestamp", "\$\$f1_points.timestamp"))
                                    )
                                )
                            )
                        )
                    )
                )
                .append("as", "joinedFlights")
        ),

        // Step 4: Unwind the joined flights
        Document("\$unwind", "\$joinedFlights"),

        // Step 5: Project the required fields
        Document(
            "\$project", Document()
                .append("f1_flightId", "\$flightId")
                .append("f2_flightId", "\$joinedFlights.flightId")
                .append("timestamp", "\$points.timestamp")
        ),

        // Step 6: Group results to ensure distinct pairs
        Document(
            "\$group", Document()
                .append("_id", Document()
                    .append("f1_flightId", "\$f1_flightId")
                    .append("f2_flightId", "\$f2_flightId")
                    .append("timestamp", "\$timestamp")
                )
        ),

        // Step 7: Sort the results
        Document(
            "\$sort", Document()
                .append("f1_flightId", 1)
                .append("f2_flightId", 1)
                .append("timestamp", 1)
        )
    )


    val queryTimeStartSecondQuery = Instant.now().toEpochMilli()
    val secondResponse = flightTripsCollection.aggregate(secondPipeline).toList()

    return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartSecondQuery, secondResponse)
}

fun BenchThread.countFlightsAtInstantInDistricts(
    staticCollections: List<MongoCollection<Document>>,
    dynamicCollections: List<MongoCollection<Document>>,
    instant: String,
    ): Quadrupel<Long, Long, Long, List<Document>>{

    // 2023-01-15 18:00:00 --> detmold 4, Koeln 2, Duesseldrof 8

    val districtsCollection = staticCollections[4]
    val flightTripsCollection = dynamicCollections[2]

    val timestamp: Date = dateFormat.parse(instant)

    val firstPipeline = listOf(
        // Step 1: Project only the `name` and `polygon` fields
        Document(
            "\$project", Document()
                .append("name", 1) // Include the `name` field
                .append("polygon.coordinates", 1) // Include the `polygon` field
                .append("_id", 0) // Exclude the `_id` field from the output
        )
    )

    val queryTimeStartFirstQuery = Instant.now().toEpochMilli()
    val firstResponse = districtsCollection.aggregate(firstPipeline).toList()
    val queryEndTimeFirstQuery = Instant.now().toEpochMilli()

    val names = mutableListOf<String>()
    val polygonCoordinates = mutableListOf<List<Any>>()

    for (result in firstResponse) {
        val name = result.getString("name")
        val polygon = result.get("polygon", Document::class.java)
        val coords = polygon?.get("coordinates")

        if (name != null) names.add(name)
        if (coords != null) polygonCoordinates.add(coords as List<Any>) // Cast to the expected type
    }

    val secondPipeline = listOf(
        // Step 1: Unwind the points array to access individual timestamps and locations
        Document("\$project", Document("\$trajectory", 0).append("points.altitude", 0)),
        Document("\$unwind", "\$points"),

        // Step 2: Match flights with the specific time instant
        Document(
            "\$match", Document(
                "points.timestamp", timestamp
            )
        ),

        // Step 3: Use $facet to count flights for each polygon
        Document(
            "\$facet", Document().apply {
                names.forEachIndexed { index, name ->
                    append(name, listOf(
                        Document(
                            "\$match", Document(
                                "points.location", Document(
                                    "\$geoWithin", Document(
                                        "\$geometry", Document("type", "Polygon").append("coordinates", polygonCoordinates[index])
                                    )
                                )
                            )
                        ),
                        Document("\$group", Document("_id", null).append("count", Document("\$sum", 1)))
                    ))
                }
            }
        ),
        Document(
            "\$project", Document().apply {
                names.forEach { name ->
                    append(name, Document(
                        "\$ifNull", listOf(
                            Document("\$arrayElemAt", listOf("\$$name.count", 0)),
                            0 // Default to 0 if no documents matched
                        )
                    ))
                }
            }
        )
    )

    val queryTimeStartSecondQuery = Instant.now().toEpochMilli()
    val secondResponse = flightTripsCollection.aggregate(secondPipeline).toList()

    return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartSecondQuery, secondResponse)


}

fun BenchThread.inCityRadiusInPeriod(
    staticCollections: List<MongoCollection<Document>>,
    dynamicCollections: List<MongoCollection<Document>>,
    period: List<String>,
    cityName: String,
    radius: Int,
): Quadrupel<Long, Long, Long, List<Document>>{
    val firstPipeline = listOf(
        Document("\$match", Document("name", cityName)),
        Document("\$project", Document("location.coordinates", 1).append("_id", 0).append("name", 1))
    )

    val citiesCollection = staticCollections[0]
    val flightTripsCollection = dynamicCollections[2]

    val startDate: Date = dateFormat.parse(period[0])
    val endDate: Date = dateFormat.parse(period[1])
    val queryTimeStartFirstQuery = Instant.now().toEpochMilli()

    val firstResponse = citiesCollection.aggregate(firstPipeline).toList()
    val queryEndTimeFirstQuery = Instant.now().toEpochMilli()

    val name = firstResponse.get(0).getString("name")
    val coordinates = firstResponse.flatMap { document ->
        val location = document.get("location", Document::class.java)
        val coords = location?.get("coordinates", List::class.java)
        coords?.map { it as Double } ?: emptyList() // Ensure null-safe mapping
    }

    val secondPipeline = listOf(
        Document("\$unwind", "\$points"),
        Document(
            "\$project", Document("trajectory", 0).append("altitude", 0)
        ),
        Document(
            "\$match", Document(
                "\$and", listOf(
                    Document("points.location",
                        Document("\$geoWithin",
                            Document("\$centerSphere", listOf(coordinates, radius))
                        )
                    ),
                    Document(
                        "points.timestamp", Document(
                            "\$gte", startDate // Start of the period
                        ).append(
                            "\$lte", endDate // End of the period
                        )
                    )
                )
            )
        ),
        Document(
            "\$group", Document(
                "_id", Document()
                    .append("flightId", "\$flightId")
                    .append("originAirport", "\$originAirport")
                    .append("destinationAirport", "\$destinationAirport")
                    .append("airplaneType", "\$airplaneType")
            )
        )
    )

    val queryTimeStartSecondQuery = Instant.now().toEpochMilli()
    val secondResponse = flightTripsCollection.aggregate(secondPipeline).toList()
    return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartSecondQuery, secondResponse)
}

// takes very long to execute as filtering based on time or space is not possible before calculating distances of each pair of flightpoints
// only way is to create smaller subset collection of flightpoints (but would also take long as check needs to happen only on docs with same timestamp)
fun BenchThread.closePairOfPlanes(
    staticCollections: List<MongoCollection<Document>>,
    dynamicCollections: List<MongoCollection<Document>>,
    period: List<String>,
    maxDistance: Int
): Quadrupel<Long, Long, Long, List<Document>> {

    val flightPointsCollection = dynamicCollections[0]

    val startDate: Date = dateFormat.parse(period[0])
    val endDate: Date = dateFormat.parse(period[1])
    val pipeline = listOf(
        // $lookup stage
        Document(
            "\$lookup", Document()
                .append("from", "flightpoints")
                .append("let", Document()
                    .append("point", "\$location")
                    .append("timestamp", "\$timestamp")
                )
                .append("pipeline", listOf(
                    Document(
                        "\$geoNear", Document()
                            .append("near", "\$\$point")
                            .append("distanceField", "dist.calculated")
                            .append("maxDistance", maxDistance)
                            .append("query", Document(
                                "\$and", listOf(
                                    Document("timestamp", "\$\$timestamp"),
                                )
                            ))
                            .append("key", "location")
                            .append("spherical", true)
                    )
                ))
                .append("as", "joinedField")
        ),
        // $match stage
        Document(
            "\$match", Document(
                "\$expr", Document(
                    "\$gt", listOf(
                        Document("\$size", "\$joinedField"),
                        0
                    )
                )
            )
        ),
        // $project stage
        Document(
            "\$project", Document()
                .append("timestamp", 1)
                .append("flightId", 1)
                .append("joinedField", 1)
        )
    )

    val queryStartTime = Instant.now().toEpochMilli()
    val firstResponse = flightPointsCollection.aggregate(pipeline).toList()
    val queryEndTimeFirstQuery = Instant.now().toEpochMilli()


    return Quadrupel(queryStartTime, 0, 0, firstResponse)

}

fun BenchThread.flightDurationInMunicipalityLowAltitudeInPeriod(
    staticCollections: List<MongoCollection<Document>>,
    dynamicCollections: List<MongoCollection<Document>>,
    period: List<String>,
    municipalityName: String,
    lowAltitude: Int
): Quadrupel<Long, Long, Long, List<Document>>{


    val municipalitiesCollection = staticCollections[2]
    val flightTripsCollection = dynamicCollections[2]

    val startDate: Date = dateFormat.parse(period[0])
    val endDate: Date = dateFormat.parse(period[1])

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

    //                        Document(
//                            "points.altitude", Document()
//                                .append("\$lt", lowAltitude) // Altitude below the given threshold
//                        )

    val secondPipeline = listOf(
        Document("\$unwind", "\$points"), // Unwind the points array
        Document(
            "\$project", Document()
                .append("trajectory", 0) // Exclude unnecessary fields
        ),
        Document(
            "\$match", Document(
                "\$and", listOf(
                    Document(
                        "points.location", Document(
                            "\$geoWithin", Document(
                                "\$geometry", Document()
                                    .append("type", "Polygon")
                                    .append("coordinates", polygonCoordinates)
                            )
                        )
                    ),
                    Document(
                        "points.timestamp", Document()
                            .append("\$gte", startDate) // Start of the period
                            .append("\$lte", endDate) // End of the period
                    )
                )
            )
        ),
        Document(
            "\$group", Document()
                .append("_id", Document()
                    .append("flightId", "\$flightId")
                    .append("originAirport", "\$originAirport")
                    .append("destinationAirport", "\$destinationAirport")
                    .append("airplaneType", "\$airplaneType")
                )
                .append(
                    "timestamps", Document(
                        "\$push", Document()
                            .append("timestamp", "\$points.timestamp")
                            .append("altitude", "\$points.altitude")
                    )
                )
        ),
        Document(
            "\$project", Document()
                .append(
                    "totalTimeBelowAltitude", Document(
                        "\$reduce", Document()
                            .append("input", "\$timestamps")
                            .append("initialValue", Document().append("total", 0).append("prev_altitude", null).append("prev_timestamp", null))
                            .append("in", Document().append("\$cond", listOf(
                                Document("\$and", listOf(
                                    Document("\$ne", listOf("\$\$value.prev_altitude", 0)), // Previous altitude is not 0
                                    Document("\$ne", listOf("\$\$value.prev_timestamp", 0)), // Previous time is not 0
                                    Document("\$ne", listOf("\$\$value.prev_altitude", null)), // Previous time is not 0
                                    Document("\$ne", listOf("\$\$value.prev_timestamp", null)), // Previous time is not 0
                                    Document("\$lt", listOf("\$\$this.altitude", lowAltitude)), // Current altitude < lowAltitude
                                    Document("\$lt", listOf("\$\$value.prev_altitude", lowAltitude)) // Previous altitude < lowAltitude
                                )),
                                Document("prev_altitude", "\$\$this.altitude").append("prev_timestamp", "\$\$this.timestamp")
                                    .append("total", Document("\$add", listOf(
                                        "\$\$value.total", 1
//                                                Document(
//                                                    "\$subtract", listOf(
//                                                        Document("\$toLong", "\$\$value.prev_timestamp"),
//                                                        Document("\$toLong", "\$\$this.timestamp"),
//                                                    )
//                                                )
                                    ))),
                                Document("prev_altitude", "\$\$this.altitude").append("prev_timestamp", "\$\$this.timestamp")

                            )))

                    ))
        ),

        Document(
            "\$project", Document()
                .append("flightId", "\$_id.flightId")
                .append("originAirport", "\$_id.originAirport")
                .append("destinationAirport", "\$_id.destinationAirport")
                .append("airplaneType", "\$_id.airplaneType")
                .append("totalTimeBelowAltitude", "\$totalTimeBelowAltitude") // Extract the total time
        )
    )

    val queryTimeStartSecondQuery = Instant.now().toEpochMilli()
    val secondResponse = flightTripsCollection.aggregate(secondPipeline).toList()
    return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartSecondQuery, secondResponse)

}

fun BenchThread.averageHourlyFlightsDuringDayInCounty(
    staticCollections: List<MongoCollection<Document>>,
    dynamicCollections: List<MongoCollection<Document>>,
    day: String,
    countyName: String
): Quadrupel<Long, Long, Long, List<Document>>{

    val countiesCollection = staticCollections[3]
    val flightTripsCollection = dynamicCollections[2]

    val startDate: Date = dateFormat.parse("$day 00:00:00")
    val endDate: Date = dateFormat.parse("$day 23:59:59")

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
            "\$project", Document("trajectory", 0).append("altitude", 0) // Exclude unnecessary fields
        ),
        Document("\$unwind", "\$points"), // Unwind the points array
        Document(
            "\$match", Document(
                "\$and", listOf(
                    Document(
                        "points.timestamp", Document()
                            .append("\$gte", startDate) // Start of the period
                            .append("\$lte", endDate) // End of the period
                    ),
                    Document(
                        "points.location", Document(
                            "\$geoWithin", Document(
                                "\$geometry", Document("type", "Polygon").append("coordinates", polygonCoordinates)
                            )
                        )
                    )
                )
            )
        ),
        Document(
            "\$project", Document()
                .append("hour", Document("\$hour", "\$points.timestamp")) // Extract hour
                .append("flightId", 1) // Include flightId
        ),
        Document(
            "\$group", Document()
                .append("_id", Document()
                    .append("hour", "\$hour")
                    .append("flightId", "\$flightId")
                ) // Group by hour and flightId
                .append("count", Document("\$sum", 1)) // Count occurrences
        ),
        Document(
            "\$group", Document()
                .append("_id", "\$_id.hour") // Group by hour
                .append("activeFlights", Document("\$sum", 1)) // Count distinct flightIds
        ),
        Document(
            "\$project", Document()
                .append("hour", "\$_id") // Set the hour
                .append("activeFlights", 1)
                .append("_id", 0)// Include the count of active flights
        ),
        Document(
            "\$sort", Document("hour", 1) // Sort by hour
        )
    )



    val queryTimeStartSecondQuery = Instant.now().toEpochMilli()
    val secondResponse = flightTripsCollection.aggregate(secondPipeline).toList()
    return Quadrupel(queryTimeStartFirstQuery, queryEndTimeFirstQuery, queryTimeStartSecondQuery, secondResponse)

}




