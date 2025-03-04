import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.KotlinFeature
import com.fasterxml.jackson.module.kotlin.KotlinModule
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.locationtech.jts.io.WKTReader
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier
import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.Statement


class DFSDataHandler(databaseName: String) {

    private val conf: BenchmarkConfiguration
    private val connection: Connection
    private val statement: Statement
    private var distributed: Boolean = false
    private val regions = listOf("municipalities", "counties", "districts")

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

        val mobilityDBIp = conf.benchmarkSettings.nodes[0]
        val connectionString = "jdbc:postgresql://$mobilityDBIp:5432/$databaseName?ApplicationName=loading-phase&autoReconnect=true&keepalives=1&keepalives_idle=120&keepalives_interval=30&keepalives_count=15"

        connection = DriverManager.getConnection(connectionString, USER, PASSWORD)
        connection.autoCommit = true
        statement = connection.createStatement()
        statement.queryTimeout = 1500

        distributed = conf.benchmarkSettings.nodes.size > 1

        println("Distributed: $distributed")


    }

    private val seps = listOf(
        0,
        700642631,
        710076001,
        718926541,
        728177911,
        736845861,
        754447851,
        760302441,
        773383481,
        774441640
    )
    private val separators = listOf(
        listOf(
            0,
            700642631,
            710076001,
            718926541,
            728177911,
            736845861,
        ),
        listOf(
            736845861,
            743346091,
            754447851,
            760302441,
            773383481,
            774441640
    ))
    private val coroutines = separators.size
    private val stepsCoroutine1 = separators[0].size - 1
    private val stepsCoroutine2 = separators[1].size - 1



    fun insertFlightPoints() {
        statement.executeUpdate(
            """
            CREATE TABLE IF NOT EXISTS flightPoints(
                flightId INTEGER, 
                timestamp TIMESTAMP WITHOUT TIME ZONE, 
                airplaneType VARCHAR(8), 
                origin VARCHAR(8), 
                destination VARCHAR(8), 
                track INTEGER,
                latitude FLOAT,
                longitude FLOAT,
                altitude FLOAT,
                Geom geography(Point, 4326)
            )
            """.trimIndent()
        )
        println("Created Table flightPoints.")

        if (distributed) {
            statement.executeQuery(
                "SELECT create_distributed_table('flightpoints', 'timestamp');"
            )
            println("Distributed Table flightPoints with timestamp as sharding key.")
        }


        statement.executeUpdate("SET DateStyle = 'ISO, DMY';")
        val copyDataFromCsv = """
            COPY flightPoints
            (flightId, timestamp, airplaneType, origin, destination, track, latitude, longitude, altitude)
            FROM '/tmp/FlightPointsMobilityDBlarge.csv' DELIMITER ',' CSV HEADER;
        """.trimIndent()
        statement.executeUpdate(copyDataFromCsv)
        println("Data copied into flightPoints table.")
        statement.executeUpdate("CREATE INDEX idx_flightpoints_flightid ON flightpoints(flightid);")
        statement.executeUpdate("CREATE INDEX idx_flightpoints_altitude ON flightpoints(altitude);")
        statement.executeUpdate("CREATE INDEX idx_flightpoints_flightid_track_timestamp ON flightpoints (flightId, track, timestamp);")
        println("Indexes created on flightpoints table for faster creation of flights table.")

    }

    fun interpolateFlightPoints() {

        statement.executeUpdate("ALTER TABLE flightPoints SET UNLOGGED;")

        statement.executeUpdate(
            """
            CREATE TABLE IF NOT EXISTS interpolatedFlightPoints(
                flightId INTEGER, 
                timestamp TIMESTAMP WITHOUT TIME ZONE, 
                airplaneType VARCHAR(8), 
                origin VARCHAR(8), 
                destination VARCHAR(8), 
                track INTEGER,
                latitude FLOAT,
                longitude FLOAT,
                altitude FLOAT,
                pointType VARCHAR(12),
                Geom geography(Point, 4326)
            )
    """.trimIndent()
        )
        println("Created Table interpolatedflightPoints.")

        if (distributed) {
            statement.executeQuery(
                "SELECT create_distributed_table('interpolatedflightpoints', 'timestamp', shard_count:=16);"
            )
            println("Distributed Table interpolatedFlightPoints with timestamp as sharding key.")
        }
        statement.executeUpdate("SET citus.max_intermediate_result_size TO '-1';")

        runBlocking {
            val interpolateJobs = (0 until coroutines).map { index1 ->
                launch(Dispatchers.IO) {
                    (0 until stepsCoroutine1).map { index2 ->
                        val flightIdLowerBound = separators[index1][index2]
                        val flightIdUpperBound = separators[index1][index2 + 1]

                        val interpolateQuery = """
                    WITH flight_pairs AS (
                        SELECT 
                            flightId, track, airplaneType, origin, destination, 
                            timestamp AS t1, 
                            LEAD(timestamp) OVER (PARTITION BY flightId, track, airplaneType, origin, destination ORDER BY timestamp) AS t2,
                            latitude AS lat1, 
                            LEAD(latitude) OVER (PARTITION BY flightId, track, airplaneType, origin, destination ORDER BY timestamp) AS lat2,
                            longitude AS lon1,
                            LEAD(longitude) OVER (PARTITION BY flightId, track, airplaneType, origin, destination ORDER BY timestamp) AS lon2,
                            altitude AS alt1,
                            LEAD(altitude) OVER (PARTITION BY flightId, track, airplaneType, origin, destination ORDER BY timestamp) AS alt2
                        FROM flightPoints
                        WHERE flightId > $flightIdLowerBound AND flightId <= $flightIdUpperBound
                    ),
                    interpolated AS (
                        SELECT 
                            f.flightId, g.timestamp::timestamp AS timestamp, f.airplaneType, f.origin, f.destination, f.track,
                              -- Explicit cast to timestamp
                            
                            ROUND(CAST(f.lat1 + ((f.lat2 - f.lat1) / 4.0) * (EXTRACT(EPOCH FROM g.timestamp - f.t1)) AS numeric), 7) AS latitude,
                            ROUND(CAST(f.lon1 + ((f.lon2 - f.lon1) / 4.0) * (EXTRACT(EPOCH FROM g.timestamp - f.t1)) AS numeric), 7) AS longitude,
                            ROUND(CAST(f.alt1 + ((f.alt2 - f.alt1) / 4.0) * (EXTRACT(EPOCH FROM g.timestamp - f.t1)) AS numeric), 1) AS altitude,      
                            'i' AS pointType,
                            NULL::geography(Point, 4326) AS Geom
                            
                        FROM flight_pairs f,
                        LATERAL generate_series(f.t1 + INTERVAL '1 second', f.t2 - INTERVAL '1 second', INTERVAL '1 second') AS g(timestamp)
                        WHERE f.t2 IS NOT NULL
                    ),
                    observed AS (
                        SELECT 
                            flightId, timestamp, airplaneType, origin, destination, track, 
                            latitude, longitude, altitude, 'o' AS pointType, Geom
                        FROM flightPoints
                        WHERE flightId > $flightIdLowerBound AND flightId <= $flightIdUpperBound
                    )
                    INSERT INTO interpolatedFlightPoints 
                    (flightId, timestamp, airplaneType, origin, destination, track, latitude, longitude, altitude, pointType, Geom)
                    SELECT * FROM observed
                    UNION ALL
                    SELECT * FROM interpolated;
                """.trimIndent()

                        statement.executeUpdate(interpolateQuery)
                        println("Interpolated missing flight points successfully inserted for $flightIdLowerBound < flightId <= $flightIdUpperBound.")


                    }
                }
            }
            interpolateJobs.joinAll()
        }
        statement.executeUpdate("CREATE INDEX idx_interpolatedflightpoints_flightid ON interpolatedflightpoints(flightid);")
        statement.executeUpdate("CREATE INDEX idx_interpolatedflightpoints_flightid_track_timestamp ON interpolatedflightpoints (flightId, track, timestamp);")
        println("Indexes created on interpolatedflightpoints table for faster creation of flights table.")
    }

    fun createGeographies() {

        runBlocking {
            val updateGeomJobs = (0 until coroutines).map { index1 ->
                launch(Dispatchers.IO) {
                    (0 until stepsCoroutine1).map { index2 ->
                        val flightIdLowerBound = separators[index1][index2]
                        val flightIdUpperBound = separators[index1][index2 + 1]

                        val updateGeoms = """
                        UPDATE flightPoints 
                        SET Geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) WHERE flightId > $flightIdLowerBound AND flightId <= $flightIdUpperBound;
            """.trimIndent()
                        statement.executeUpdate(updateGeoms)
                        println("Updated Geoms flightpoints table.")

                    }
                }
            }
            updateGeomJobs.joinAll()
        }

        runBlocking {
            val updateGeomJobsInterpol = (0 until coroutines).map { index1 ->
                launch(Dispatchers.IO) {
                    (0 until stepsCoroutine1).map { index2 ->
                        val flightIdLowerBound = separators[index1][index2]
                        val flightIdUpperBound = separators[index1][index2 + 1]

                        val updateGeoms = """
                            UPDATE interpolatedFlightPoints 
                            SET Geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) WHERE flightId > $flightIdLowerBound AND flightId <= $flightIdUpperBound;
            """.trimIndent()
                        statement.executeUpdate(updateGeoms)
                        println("Updated Geoms in interpolatedflightpoints table.")

                    }
                }
            }
            updateGeomJobsInterpol.joinAll()
        }

    }

    fun createFlightTrips() {

        statement.executeUpdate(
            """
            CREATE TABLE IF NOT EXISTS flights(
                flightId INTEGER,
                airplaneType TEXT,
                origin TEXT,
                destination TEXT,
                track INTEGER,
                altitude tfloat,
                trip tgeogpoint,
                observedtrip tgeogpoint
            );
            """.trimIndent()
        )
        println("Created table flights.")

        if (distributed){

            statement.executeQuery(
                "SELECT create_distributed_table('flights', 'flightid', shard_count:=16);")

        }

            val query =
                """
            INSERT INTO flights(flightId, airplaneType, origin, destination, track, altitude, trip, observedtrip)
            SELECT 
                flightId,
                airplaneType,
                origin,
                destination,
                track,
                tfloatSeq(
                    array_agg(tfloat(altitude, timestamp) ORDER BY timestamp) 
                    FILTER (WHERE altitude IS NOT NULL), 'step'
                ),
                tgeogpointseq(
                    array_agg(tgeogpoint(Geom, timestamp) ORDER BY timestamp) 
                    FILTER (WHERE track IS NOT NULL), 'step'
                ), 
                tgeogpointseq(
                    array_agg(tgeogpoint(Geom, timestamp) ORDER BY timestamp)
                    FILTER (WHERE pointtype = 'o' AND track IS NOT NULL), 'linear'
                )
            FROM interpolatedflightpoints WHERE flightid < 736845861
            GROUP BY flightId, track, airplaneType, origin, destination;
        """.trimIndent()

            statement.executeUpdate(query)

        val secondQuery =
            """
            INSERT INTO flights(flightId, airplaneType, origin, destination, track, altitude, trip, observedtrip)
            SELECT 
                flightId,
                airplaneType,
                origin,
                destination,
                track,
                tfloatSeq(
                    array_agg(tfloat(altitude, timestamp) ORDER BY timestamp) 
                    FILTER (WHERE altitude IS NOT NULL), 'step'
                ),
                tgeogpointseq(
                    array_agg(tgeogpoint(Geom, timestamp) ORDER BY timestamp) 
                    FILTER (WHERE track IS NOT NULL), 'step'
                ), 
                tgeogpointseq(
                    array_agg(tgeogpoint(Geom, timestamp) ORDER BY timestamp)
                    FILTER (WHERE pointtype = 'o' AND track IS NOT NULL), 'step'
                )
            FROM interpolatedflightpoints WHERE flightid >= 736845861
            GROUP BY flightId, track, airplaneType, origin, destination;
        """.trimIndent()

        statement.executeUpdate(secondQuery)

        println("Dropping table interpolatedlflightpoints.")
        statement.executeUpdate("DROP TABLE interpolatedflightpoints CASCADE")

        statement.executeUpdate("CREATE INDEX idx_flights_trip_gist ON flights USING GIST(trip);")



        try {
            val resultSet = statement.executeQuery("SELECT COUNT(*) FROM flights;")
            if (resultSet.next()) {
                println("Flights table contains trajectories for ${resultSet.getInt(1)} flight trips.")
            }

        } catch (e: Exception) {
            println("Could not count flight trajectories.")
            e.printStackTrace()
        }
    }

    fun processStaticData() {
        insertCityData()
        insertAirportData()

        regions.forEach {
            insertRegionalData(it)
        }

    }

    private fun insertCityData() {
        try {
            statement.executeUpdate(
                """
                CREATE TABLE cities (
                    area NUMERIC(8, 3),
                    lat NUMERIC(8, 5),
                    lon NUMERIC(8, 5),
                    district VARCHAR(50),
                    name VARCHAR(50),
                    population INTEGER,
                    Geom geography(Point, 4326)
                )
                """.trimIndent()
            )

            if (distributed) {
                try {
                    statement.executeQuery(
                        "SELECT create_reference_table('cities');"
                    )
                    println("Replicated cities table across all machines.")
                } catch (e: Exception) {
                    println("Could not distribute table cities.")
                    e.printStackTrace()
                }
            }

            statement.executeUpdate(
                """
                COPY cities (area, lat, lon, district, name, population)
                FROM '/tmp/regData/cities.csv' DELIMITER ',' CSV HEADER;
                """.trimIndent()
            )
            statement.executeUpdate(
                """
                UPDATE cities 
                SET Geom = ST_SetSRID(ST_MakePoint(lon, lat), 4326);
                """.trimIndent()
            )
            println("Inserted city data.")


        } catch (e: Exception) {
            println("Could not create or populate cities table.")
            e.printStackTrace()
        }

    }

    private fun insertAirportData(){
        println("Creating Table for airports.")
        try {

            statement.executeUpdate("""
                CREATE TABLE airports (
                    IATA CHAR(3) NOT NULL,
                    ICAO CHAR(4),
                    AirportName VARCHAR(255) NOT NULL,
                    Country VARCHAR(100) NOT NULL,
                    City VARCHAR(100)
                )
                """.trimIndent()
            )

            if (distributed) {
                try {
                    statement.executeQuery(
                        "SELECT create_reference_table('airports');"
                    )
                    println("Replicated airports table across all machines.")
                } catch (e: Exception) {
                    println("Could not distribute table airports.")
                    e.printStackTrace()
                }
            }

            statement.executeUpdate("""
                COPY airports (IATA, ICAO, AirportName, Country, City)
                FROM '/tmp/regData/airports.csv' 
                DELIMITER ';' 
                CSV HEADER 
                ENCODING 'WIN1252';
                """.trimIndent()
            )

        }catch (e: Exception){
            e.printStackTrace()
        }
    }

    private fun simplifyPolygonWKT(wkt: String, tolerance: Double): String {
        val reader = WKTReader()
        val geometry: org.locationtech.jts.geom.Geometry = reader.read(wkt)

        // Ensure it does not log to console by accident
        if (!geometry.isValid || geometry.isEmpty) {
            throw IllegalArgumentException("Invalid or empty geometry")
        }

        val simplifiedGeometry = DouglasPeuckerSimplifier.simplify(geometry, tolerance)

        // Explicitly return the WKT string, no printing
        return simplifiedGeometry.toText()
    }

    private fun insertRegionalData(region: String) {
        try {
            println("Processing regional data for $region.")

            statement.executeUpdate("""
                CREATE TABLE $region (
                id SERIAL PRIMARY KEY, 
                name VARCHAR(255) NOT NULL, 
                geom geography(POLYGON, 4326)
                )
            """.trimIndent())

            val csvFile = "/$region-wkt.csv"
            val inputStream = object {}.javaClass.getResourceAsStream(csvFile)
                ?: throw IllegalArgumentException("File $csvFile not found!")

            val reader = InputStreamReader(inputStream, StandardCharsets.UTF_8)
            val lines = reader.readLines().drop(1)  // Skip header

            val sql = "INSERT INTO $region (name, geom) VALUES (?, ST_GeomFromText(?, 4326))"

            connection.autoCommit = false
            var preparedStatement: PreparedStatement = connection.prepareStatement(sql)

            for (line in lines) {
                val parts = line.split(";")

                val name = parts[0].trim()
                val wkt = parts[1].trim()

                val simplifiedWkt = simplifyPolygonWKT(wkt, .008)

                preparedStatement.setString(1, name)
                preparedStatement.setString(2, simplifiedWkt)
                preparedStatement.addBatch()
            }

            preparedStatement.executeBatch()
            connection.commit()

            connection.autoCommit = true


            if (distributed) {
                try {
                    val sql = "SELECT create_reference_table('$region');"
                    statement.executeQuery(
                        sql
                    )
                    println("Replicated $region table across all machines.")
                } catch (e: Exception) {
                    println("Could not distribute table $region.")
                    e.printStackTrace()
                }
            }


            println("Inserted data for $region.")
            println("Processed region data for $region.")

        } catch (e: Exception) {
            println("Error processing region data for $region.")
            e.printStackTrace()
        }
    }

    fun createIndexes(){

        createFlightPointsIndex()
        createFlightTripsIndex()
        createStaticTablesIndexes()
        println("Finished index creation.")

    }

    private fun createFlightPointsIndex(){

        statement.executeUpdate("CREATE INDEX idx_flightpoints_timestamp ON flightpoints(timestamp);")
        statement.executeUpdate("CREATE INDEX idx_flightpoints_geom_gist ON flightpoints USING gist(geom);")
        println("Created Indexes for flightpoints.")
    }

    private fun createFlightTripsIndex(){


       // statement.executeUpdate("CREATE INDEX idx_flights_trip_spgist ON flights USING SPGIST (trip);")
        statement.executeUpdate("CREATE INDEX idx_flights_altitude_gist ON flights USING GIST(altitude);")
        statement.executeUpdate("CREATE INDEX idx_flights_flightid ON flights (flightid);")
        statement.executeUpdate("CREATE INDEX idx_flights_traj_gist ON flights USING gist(traj)")
        statement.executeUpdate("CREATE INDEX idx_flights_origin_hash ON flights USING hash(origin);")
        statement.executeUpdate("CREATE INDEX idx_flights_destination_hash ON flights USING hash(destination);")


        //TODO: Index on f.trip

        println("Created Indexes for flights.")

    }

    private fun createStaticTablesIndexes(){

        // municipalites, counties, districts index creation
        regions.forEach{

            val regionsIndexQuery = """
            CREATE INDEX idx_${it}_name_hash
            ON $it USING hash(name);
            """
            statement.executeUpdate(regionsIndexQuery)

            val regionsGeomIndexQuery = """
                CREATE INDEX idx_${it}_geom_gist ON ${it} USING gist(geom);
            """.trimIndent()
            statement.executeUpdate(regionsGeomIndexQuery)

        }

        println("Created indexes for regions.")

        // city table index creation
        val cityIndexQuery = """
        CREATE INDEX idx_city_name_hash
        ON cities USING hash(name);
        """
        statement.executeUpdate(cityIndexQuery)

        val cityGeomIndexQuery =
            """
                CREATE INDEX idx_city_geom_gist ON cities USING gist(geom);
            """.trimIndent()
        statement.executeUpdate(cityGeomIndexQuery)

        println("Created indexes for cities.")

        // airport table index creation
        val airportIndexQuery1 = """
            CREATE INDEX idx_airport_icao_hash
            ON airports USING hash(icao);
        """.trimIndent()
        statement.executeUpdate(airportIndexQuery1)

        val airportIndexQuery2 = """
            CREATE INDEX idx_airport_city_hash
            ON airports USING hash(city);
        """.trimIndent()
        statement.executeUpdate(airportIndexQuery2)

        println("Created indexes for airports.")
    }

    fun createTrajectoryColumn() {

        statement.executeUpdate("ALTER TABLE flights ADD COLUMN traj GEOGRAPHY;")
        statement.executeUpdate("UPDATE flights SET traj = trajectory(observedtrip)")
        statement.executeUpdate("ALTER TABLE flights DROP COLUMN IF EXISTS observedtrip;")
        println("Added and populated traj column in flights table.")

    }

}

fun main() {
    val handler = DFSDataHandler("aviation_data")
//    handler.processStaticData()
//    handler.insertFlightPoints()
//    handler.interpolateFlightPoints()
//    handler.createGeographies()
//    handler.createFlightTrips()
    handler.createTrajectoryColumn()
    handler.createIndexes()

}
