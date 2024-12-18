package dfsData

import BenchmarkConfiguration
import BenchmarkSettings
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.KotlinFeature
import com.fasterxml.jackson.module.kotlin.KotlinModule
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement

class DFSDataHandler {

    private val conf: BenchmarkConfiguration
    private val connection: Connection
    private val statement: Statement

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
        val connectionString = "jdbc:postgresql://$mobilityDBIp:5432/aviation_data"
        val user = "felix"
        val password = "master"

        connection = DriverManager.getConnection(connectionString, user, password)
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
                    "SELECT create_distributed_table('flightpoints', 'flightid');"
                )
                println("Distributed Table flightPoints with flightId as sharding key.")
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
        val updatedRows = statement.executeUpdate(updateGeoms)
    }

    private fun createTrajectories() {
        try {
            statement.executeUpdate(
                """
                CREATE TABLE flights(flightId, airplaneType, origin, destination, track, altitude, trip) AS
                SELECT 
                    flightId,
                    airplaneType,
                    origin, 
                    destination,
                    track,
                    tfloatSeq(array_agg(tfloat(altitude, timestamp) ORDER BY timestamp) FILTER (WHERE altitude IS NOT NULL), 'step'),
                    tgeompointSeq(array_agg(tgeompoint(ST_Transform(Geom, 4326), timestamp) ORDER BY timestamp) FILTER (WHERE track IS NOT NULL), 'step')
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

    fun processLocationsData() {
        insertStateData()
        insertAirportData()

    }

    private fun insertStateData() {
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
                    Geom GEOMETRY(Point, 25832)
                )
                """.trimIndent()
            )
            statement.executeUpdate(
                """
                COPY cities (area, lat, lon, district, name, population)
                FROM '/tmp/regData/cities.csv' DELIMITER ',' CSV HEADER;
                """.trimIndent()
            )
            statement.executeUpdate(
                """
                UPDATE cities 
                SET Geom = ST_Transform(ST_SetSRID(ST_MakePoint(lon, lat), 4326), 25832);
                """.trimIndent()
            )
            println("Inserted city data.")
        } catch (e: Exception) {
            println("Could not create or populate cities table.")
            e.printStackTrace()
        }

        listOf("municipalities", "counties", "districts").forEach {
            createRegTable(it)
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

    private fun createRegTable(region: String) {
        try {
            println("Processing region data for $region.")
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

            statement.executeUpdate("DROP TABLE temp_$region;")
            statement.executeUpdate("ALTER TABLE $region ADD COLUMN geom_etrs GEOMETRY(Polygon, 25832);")
            statement.executeUpdate("UPDATE $region SET geom_etrs = ST_Transform(Geom, 25832);")
            println("Processed region data for $region.")
        } catch (e: Exception) {
            println("Error processing region data for $region.")
            e.printStackTrace()
        }
    }
}

fun main() {
    val handler = DFSDataHandler()
    handler.processFlightData()
    handler.processLocationsData()
}
