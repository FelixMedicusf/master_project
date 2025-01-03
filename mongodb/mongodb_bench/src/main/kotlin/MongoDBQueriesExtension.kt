import com.mongodb.client.MongoCollection
import org.bson.Document
import java.text.SimpleDateFormat
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.*

private val dateFormat = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

fun BenchThread.queryFlightsByPeriod(
    flightTripsCollection: MongoCollection<Document>,
    vararg period: String): Pair<Long,
        List<Document>> {

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

// temporal queries
fun BenchThread.locationOfAirplaneAtInstant(
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
                )))
        )
    )

    val queryTimeStart = Instant.now().toEpochMilli()
    // Execute the aggregation
    val results = flightTripsCollection.aggregate(pipeline)

    // Convert results to a list of Documents
    return Pair(queryTimeStart, results.into(mutableListOf()))
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

// spatial queries
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
    low_altitude: Int,
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

