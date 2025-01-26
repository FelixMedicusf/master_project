package dfsData

import BenchmarkConfiguration
import PASSWORD
import USER
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.KotlinFeature
import com.fasterxml.jackson.module.kotlin.KotlinModule
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager
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
        val connectionString = "jdbc:postgresql://$mobilityDBIp:5432/$databaseName"

        connection = DriverManager.getConnection(connectionString, USER, PASSWORD)
        connection.autoCommit = true
        statement = connection.createStatement()
        statement.queryTimeout = 1500

        //var distributed = conf.benchmarkSettings.nodes.size > 1
        distributed = true


    }


    fun processFlightData() {
        insertFlightPoints()
        createTrajectories()
    }

    private fun insertFlightPoints() {
        try {
            statement.executeUpdate(
                """
                CREATE TABLE flightPoints(
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
        } catch (e: Exception) {
            println("Could not create Table flightPoints.")
            e.printStackTrace()
        }

        if (distributed) {
            try {
                statement.executeQuery(
                    "SELECT create_distributed_table('flightpoints', 'timestamp');"
                )
                println("Distributed Table flightPoints with timestamp as sharding key.")
            } catch (e: Exception) {
                println("Could not distribute table flightPoints.")
                e.printStackTrace()
            }
        }

        try {
            statement.executeUpdate("SET DateStyle = 'ISO, DMY';")
            val copyDataFromCsv = """
                COPY flightPoints
                (flightId, timestamp, airplaneType, origin, destination, track, latitude, longitude, altitude)
                FROM '/tmp/FlightPointsMobilityDBlarge.csv' DELIMITER ',' CSV HEADER;
            """.trimIndent()
            statement.executeUpdate(copyDataFromCsv)
            println("Data copied into flightPoints table.")
            statement.executeUpdate("CREATE INDEX idx_flightpoints_flightid ON flightpoints(flightid);")
            statement.executeUpdate("CREATE INDEX idx_flightpoints_flightid_track_timestamp ON flightpoints (flightId, track, timestamp);")
            println("Indexes created on flightpoints table for faster creation of flights table.")

        } catch (e: Exception) {
            println("Could not load data into flightPoints table.")
            e.printStackTrace()
        }

        statement.executeUpdate("ALTER TABLE flightPoints SET UNLOGGED;")


        var updateGeoms = """
                UPDATE flightPoints 
                SET Geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);
            """.trimIndent()


        statement.executeUpdate(updateGeoms)


        println("Updated Geoms in flightpoints table.")
    }

    private fun createTrajectories() {

        statement.executeUpdate(
            """
            CREATE TABLE flights (
                flightId INTEGER,
                airplaneType TEXT,
                origin TEXT,
                destination TEXT,
                track INTEGER,
                altitude tfloat,
                trip tgeogpoint
            );
            """.trimIndent()
        )
        println("Created table flights.")

        statement.executeUpdate("ALTER TABLE flights SET UNLOGGED;")

        if (distributed) {
            try {

//                statement.executeUpdate("ALTER TABLE flights ADD COLUMN shard_key BIGINT;")
//                statement.executeUpdate("CREATE SEQUENCE flights_shard_seq;")
//                statement.executeUpdate("UPDATE flights SET shard_key = nextval('flights_shard_seq');")

                statement.executeQuery(
                    "SELECT create_distributed_table('flights', 'flightid');"

                )
                println("Distributed Table flights with flightid as sharding key.")

            } catch (e: Exception) {
                println("Could not distribute table flightPoints.")
                e.printStackTrace()
            }
        }


        try {
            val query =
                """
                INSERT INTO flights (flightId, airplaneType, origin, destination, track, altitude, trip)
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
                    tgeogpointSeq(
                        array_agg(tgeogpoint(Geom, timestamp) ORDER BY timestamp) 
                        FILTER (WHERE track IS NOT NULL), 'step'
                    )
                FROM flightpoints
                GROUP BY flightId, track, airplaneType, origin, destination;
                """.trimIndent()

            statement.executeUpdate(query)
            println("Inserted data into flights table.")

        } catch (e: Exception) {
            println("Could not insert data into flights table.")
            e.printStackTrace()
        }


//        statement.executeUpdate("CLUSTER flights USING trip_gist_idx;")
//        println("Clustered flights by gist index.")



        try {
            statement.executeUpdate("ALTER TABLE flights ADD COLUMN traj GEOGRAPHY;")
            statement.executeUpdate("UPDATE flights SET traj = trajectory(trip);")
            println("Added and populated traj column in flights table.")
        } catch (e: Exception) {
            println("Could not create or populate traj column.")
            e.printStackTrace()
        }

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

    private fun insertRegionalData(region: String) {
        try {
            println("Processing regional data for $region.")
            statement.executeUpdate(
                """
                CREATE TEMP TABLE temp_$region (
                    row_number SERIAL PRIMARY KEY,
                    name VARCHAR(255),
                    latitude DOUBLE PRECISION,
                    longitude DOUBLE PRECISION
                );
                """.trimIndent()
            )
            statement.executeUpdate(
                """
                COPY temp_$region(name, latitude, longitude)
                FROM '/tmp/regData/$region.csv' WITH (FORMAT csv, HEADER true);
                """.trimIndent()
            )
            statement.executeUpdate(
                """
                CREATE TABLE $region (
                    name VARCHAR(255),
                    Geom geography(Polygon, 4326) NOT NULL
                );
                """.trimIndent()
            )

            if (distributed) {
                try {
                    statement.executeQuery(
                        "SELECT create_reference_table('${region}');"
                    )
                    println("Replicated $region table across all machines.")
                } catch (e: Exception) {
                    println("Could not distribute table $region.")
                    e.printStackTrace()
                }
            }

            statement.executeUpdate("""
                    INSERT INTO $region (name, Geom)
                    SELECT
                        name,
                        ST_MakePolygon(
                            ST_GeomFromText(
                                'LINESTRING(' || 
                                string_agg(latitude || ' ' || longitude, ', ' ORDER BY row_number) || 
                                ')'
                            )
                        )::geography(Polygon, 4326)
                    FROM temp_$region
                    GROUP BY name;
                    """.trimIndent()
            )

            println("Inserted data for $region.")

            statement.executeUpdate("DROP TABLE temp_$region;")

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

        statement.executeUpdate("CREATE INDEX trip_gist_idx ON flights USING GIST(trip);")
        statement.executeUpdate("CREATE INDEX idx_flights_trip_spgist ON flights USING SPGIST (trip);")
        statement.executeUpdate("CREATE INDEX idx_flights_traj_gist ON flights USING gist(traj)")
        statement.executeUpdate("CREATE INDEX idx_flights_origin_hash ON flights USING hash(origin);")
        statement.executeUpdate("CREATE INDEX idx_flights_destination_hash ON flights USING hash(destination);")
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

}

fun main() {
    val handler = DFSDataHandler("aviation_data")
    handler.processStaticData()
    handler.processFlightData()
    handler.createIndexes()
}
