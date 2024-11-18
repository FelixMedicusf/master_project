package dfsData
import java.sql.DriverManager
import java.sql.Statement

val distributedDatabase = true

val mobilityDBIp = "34.38.56.113"

val connectionString = "jdbc:postgresql://$mobilityDBIp:5432/aviation_data"
val user = "felix"
val password = "master"

val connection = DriverManager.getConnection(connectionString, user, password)
val statement = connection.createStatement()


fun main(){
    insertFlightPoints()
    createTrajectories()
    insertStateData()
}
// val header = "flightId,timestamp,airplaneType,originAirport,destinationAirport,track,latitude,longitude,altitude"

fun insertFlightPoints(){

    try {

        val createTable = """
            CREATE TABLE flightPoints(
                flightId integer, 
                timestamp Timestamp, 
                airplaneType varchar(8), 
                origin varchar(8), 
                destination varchar(8), 
                track varchar(8),
                latitude float,
                longitude float,
                altitude float,
                Geom geometry(Point, 25832)
        )
        """.trimIndent()

        val tablesAffected = statement.executeUpdate(createTable)
        println("Created Table flightInput.")

    }catch (e: Exception){
        println("Could not create Table flightInput.")
        e.printStackTrace()

    }


    if (distributedDatabase) {
        try {

            val distributeTables = """
            SELECT create_distributed_table('flightpoints', 'flightid');
            """.trimIndent()

            val distributeQueryResult = statement.executeQuery(distributeTables)

            println("Distributed Table flightInput with flightId as sharding key.")

        }catch (e: Exception) {
            println("Could not distribute table flightInput.")
            e.printStackTrace()

        }

    }

    try {
        val setDateStyle = """
            SET DateStyle = 'ISO, DMY';
        """.trimIndent()

        statement.executeUpdate(setDateStyle)

        val copyDataFromCsvFile = """
            COPY flightPoints
            (flightId, 
             timestamp, 
             airplaneType, 
             origin, 
             destination, 
             track, 
             latitude, 
             longitude, 
             altitude
             ) FROM '/tmp/FlightPointsMobilityDB.csv' DELIMITER ',' CSV HEADER;
        """.trimIndent()

        println("Copying flight data from csv file into the database ...")

        val copyResponse = statement.executeUpdate(copyDataFromCsvFile)
        println("Number of added Records: $copyResponse")
    }
    catch (e: Exception) {
        println("Could not load data from csv file into flightInput table.")
    }

    try {
        val updateGeoms = """
            SET Geom = ST_Transform(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326), 25832);
        """.trimIndent()

        println("Updating database to hold geometry points made of latitude and longitude.")
        println("Number of updated rows: " + statement.executeUpdate(updateGeoms))

    }catch (e: Exception){
        println("Could not update Geometry Points.")
    }

}
fun createTrajectories () {

    try{

        val createTrajectories = """
            CREATE TABLE flights(flightId, airplaneType, origin, destination, track, altitude, trip) AS
            SELECT flightId,
            airplaneType,
            origin, 
            destination,
            ttextSeq(array_agg(ttext(track, timestamp) ORDER BY timestamp) FILTER (WHERE track IS NOT NULL)),
            tfloatSeq(array_agg(tfloat(altitude, timestamp) ORDER BY timestamp) FILTER (WHERE altitude IS NOT NULL)),
            tgeompointSeq(array_agg(tgeompoint( ST_Transform(Geom, 25832), timestamp) ORDER BY timestamp) FILTER (WHERE track IS NOT NULL))
            FROM flightPoints 
            GROUP BY flightId, airplaneType, origin, destination;
        """.trimIndent()

        println("Create Table flights to build trajectories for flight data.")
        statement.executeUpdate(createTrajectories)

    }catch(e: Exception){

        println("Could not create table for trajectories.")
        e.printStackTrace()
    }

    try {
        val alterTableflights = """
            ALTER TABLE flights ADD COLUMN traj geometry;
        """.trimIndent()

        val alterTraj = """
            UPDATE flights SET traj = trajectory(trip);
        """.trimIndent()

        println("Create column for trajectories in flights table.")
        statement.executeUpdate(alterTableflights)
        statement.executeUpdate(alterTraj)

    }catch (e: Exception){
        println("Could not create column Traj.")
        e.printStackTrace()
    }

    try{
        val countFlightTrips = """
            SELECT COUNT(*) FROM flights;
        """.trimIndent()

        val resultSet = statement.executeQuery(countFlightTrips)

        var numberOfRows = 0
        if (resultSet.next()) {  // Move the cursor to the first row in the result set
            numberOfRows = resultSet.getInt(1) // Get the value of the first column in the result set
        }

        println("Flight Table now contains Trajectories for $numberOfRows flight trips.")

    }catch (e: Exception){
        println("Could not execute COUNT query.")
        e.printStackTrace()
    }
}

fun insertStateData(){

    try {

    val createTable = """
        CREATE TABLE cityData (
            area NUMERIC(8, 3),
            lat NUMERIC(8, 5),
            lon NUMERIC(8, 5),
            district VARCHAR(50),
            name VARCHAR(50),
            population INTEGER,
            Geom geometry(Point, 4326)
        )
        """.trimIndent()

    val tablesAffected = statement.executeUpdate(createTable)

    val copyDataFromCsvFile = """
            COPY cityData
            (area, 
             lat, 
             lon, 
             district, 
             name, 
             population
             ) FROM '/tmp/staedte.csv' DELIMITER ',' CSV HEADER;
        """.trimIndent()

    statement.executeUpdate(copyDataFromCsvFile)

    val updateCityGeoms = """
            UPDATE cityData SET Geom = ST_SetSRID( ST_MakePoint( lon, lat ), 25832);
        """.trimIndent()

    statement.executeUpdate(updateCityGeoms)


    }catch (e: Exception){
        e.printStackTrace()
        println("Could not create table containing cities")
    }

    createRegTable(statement, "gemeinden")
    createRegTable(statement, "kreise")
    createRegTable(statement, "regierungsbezirke")

}
fun createRegTable(statement: Statement, region: String){
    var createTempTable = """
        CREATE TEMP TABLE temp_$region (
            name VARCHAR(255),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION
        );
    """.trimIndent()

    statement.executeQuery(createTempTable)

    var copyTempData = """
        COPY temp_$region(name, latitude, longitude)
        FROM '/tmp/$region.csv' WITH (FORMAT csv, HEADER true
        );
        """.trimIndent()

    statement.executeQuery(copyTempData)

    var createRegTable = """
    CREATE TABLE $region (               
        name VARCHAR(255),              
        Geom Geometry(Polygon) NOT NULL
    )
    """.trimIndent()

    statement.executeUpdate(createRegTable)

    var fillRegTable = """
        INSERT INTO $region (name, Geom)
        SELECT name,
               ST_GeomFromText(
                   'POLYGON((' || string_agg(longitude || ' ' || latitude, ', ') || '))', 4326
               )
        FROM temp_$region
        GROUP BY name;
    """.trimIndent()

    statement.executeUpdate(fillRegTable)

    var dropTempTable = """
        DROP TABLE IF EXISTS temp_$region;
    """.trimIndent()

    statement.executeUpdate(dropTempTable)

    var updateSRID = """
        UPDATE $region
        SET Geom = ST_Transform(Geom, 25832)
        WHERE ST_SRID(Geom) = 4326;
    """.trimIndent()

    statement.executeUpdate(updateSRID)

}
