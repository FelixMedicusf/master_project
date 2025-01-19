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
        statement = connection.createStatement()
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
                    timestamp TIMESTAMP, 
                    airplaneType VARCHAR(8), 
                    origin VARCHAR(8), 
                    destination VARCHAR(8), 
                    track VARCHAR(8),
                    latitude FLOAT,
                    longitude FLOAT,
                    altitude FLOAT,
                    Geom GEOMETRY(Point, 4326)
                )
                """.trimIndent()
            )
            println("Created Table flightPoints.")
        } catch (e: Exception) {
            println("Could not create Table flightPoints.")
            e.printStackTrace()
        }

        if (conf.benchmarkSettings.nodes.size != 1) {
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
                FROM '/tmp/FlightPointsMobilityDB.csv' DELIMITER ',' CSV HEADER;
            """.trimIndent()
            statement.executeUpdate(copyDataFromCsv)
            println("Data copied into flightPoints table.")
        } catch (e: Exception) {
            println("Could not load data into flightPoints table.")
            e.printStackTrace()
        }

        /*
        try {
            val updateGeoms = """
                UPDATE flightPoints 
                SET Geom = ST_Transform(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326), 25832);
            """.trimIndent()
            val updatedRows = statement.executeUpdate(updateGeoms)
            println("Updated $updatedRows rows with geometry points.")
        } catch (e: Exception) {
            println("Could not update Geometry Points.")
            e.printStackTrace()
        }

         */
        val updateGeoms = """
                UPDATE flightPoints 
                SET Geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);
            """.trimIndent()
        statement.executeUpdate(updateGeoms)
    }

    private fun createTrajectories() {

        statement.executeUpdate(
            """
            CREATE TABLE flights (
                flightId TEXT,
                airplaneType TEXT,
                origin TEXT,
                destination TEXT,
                track TEXT,
                altitude tfloatSeq,
                trip tgeogpointSeq
            );
            """.trimIndent()
        )

        // TODO: See if I can shard on trip column
        if (conf.benchmarkSettings.nodes.size != 1) {
            try {
                statement.executeQuery(
                    "SELECT create_distributed_table('flights', 'trip');"
                    // TODO: check how I can shard on trip column
                )
                println("Distributed Table flightPoints with timestamp as sharding key.")
            } catch (e: Exception) {
                println("Could not distribute table flightPoints.")
                e.printStackTrace()
            }
        }

        try {
            statement.executeUpdate(
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
                            array_agg(tgeogpoint(ST_Transform(Geom, 4326), timestamp) ORDER BY timestamp) 
                            FILTER (WHERE track IS NOT NULL), 'step'
                        )
                    FROM flightPoints
                    GROUP BY flightId, track, airplaneType, origin, destination;
                """.trimIndent()
            )

            println("Created flights table with trajectories.")

        } catch (e: Exception) {
            println("Could not create flights table.")
            e.printStackTrace()
        }


        try {
            statement.executeUpdate("ALTER TABLE flights ADD COLUMN traj GEOMETRY;")
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
                    Geom GEOMETRY(Point, 4326)
                )
                """.trimIndent()
            )

            if (conf.benchmarkSettings.nodes.size != 1) {
                try {
                    statement.executeQuery(
                        "SELECT create_distributed_table('cities', 'name', 'hash');"
                    )
                    println("Distributed table cities with hashed name as sharding key.")
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


            if (conf.benchmarkSettings.nodes.size != 1) {
                try {
                    statement.executeQuery(
                        "SELECT create_distributed_table('airports', 'icao', 'hash');"
                    )
                    println("Distributed Table airports with hashed icao as sharding key.")
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
                    Geom GEOMETRY(Polygon, 4326) NOT NULL
                );
                """.trimIndent()
            )

            if (conf.benchmarkSettings.nodes.size != 1) {
                try {
                    statement.executeQuery(
                        "SELECT create_distributed_table('${region}', 'name', 'hash');"
                    )
                    println("Distributed Table $region with hashed name as sharding key.")
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
                        )::geometry(Polygon, 4326)
                    FROM temp_$region
                    GROUP BY name;
                    """.trimIndent()
            )

            println("Inserted data for $region.")

            statement.executeUpdate("DROP TABLE temp_$region;")

            //statement.executeUpdate("ALTER TABLE $region ADD COLUMN geom_etrs GEOMETRY(Polygon, 25832);")
            //statement.executeUpdate("UPDATE $region SET geom_etrs = ST_Transform(Geom, 25832);")
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

    }

    private fun createFlightPointsIndex(){
        // TODO: add Gist and/or SP-Gist Index for geom column
        // TODO: add ascending Index for timestamp column

        //GiST and SP-GiST
        //CREATE INDEX flights_trip_Gist_Idx ON flights USING Gist(trip);
        //CREATE INDEX flights_trip_SPGist_Idx ON flights USING SPGist(trip);

//        In GiST partitioning, we use the cluster operation
//        of PostgreSQL to redistribute the data of the table using the GiST
//                index. This operation exploits the index to store the trips that are
//        close to each other together in the same table

    }

    private fun createFlightTripsIndex(){
        // TODO: add Gist and/or SP-Gist Index for trip column
        // TODO: add index for traj column
        // TODO: shard on gist index or sp-gist index


    }

    private fun createStaticTablesIndexes(){

        regions.forEach{

            val regionsIndexQuery = """
            CREATE INDEX ${it}_name_hash_index
            ON $it USING hash (name);
            """

            statement.executeUpdate(regionsIndexQuery)
        }

        val cityIndexQuery = """
        CREATE INDEX city_name_hash_index
        ON cities USING hash (name);
        """

        statement.executeUpdate(cityIndexQuery)

        val airportIndexQuery = """
            CREATE INDEX airport_icao_hash_index
            ON airports USING hash (icao);
        """.trimIndent()

        statement.executeUpdate(airportIndexQuery)




    }

}

fun main() {
    val handler = DFSDataHandler("aviation_data")
    handler.processFlightData()
    handler.processStaticData()
    handler.createIndexes()
}
