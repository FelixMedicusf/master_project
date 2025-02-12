package EuroControlData

import java.sql.DriverManager

val distributedDatabase = true

val mobilityDBIp = "34.22.178.156"

val connectionString = "jdbc:postgresql://$mobilityDBIp:5432/aviation_data"
val user = "felix"
val password = "master_thesis"

val connection = DriverManager.getConnection(connectionString, user, password)
val statement = connection.createStatement()


fun main(){
    loadData()
    createTrajectories()
}


fun createTrajectories () {

    try{

        val createTrajectories = """
            CREATE TABLE flights(ectrlnumber, trip, flightLevel) AS
            SELECT ectrlnumber, 
            tgeompointseq(array_agg(tgeompoint( ST_Transform(Geom, 4326), timestamp) ORDER BY timestamp)),
            tfloatseq(array_agg(tfloat(flightLevel, timestamp) ORDER BY timestamp) FILTER (WHERE flightLevel IS NOT NULL)) 
            FROM flightInput 
            GROUP BY ectrlnumber;
        """.trimIndent()

        println("Create Table flights to build trajectories for flight data.")
        statement.executeUpdate(createTrajectories)

    }catch(e: Exception){

        println("Could not create table for trajectories.")
        e.printStackTrace()
    }

    try {
        val alterTableflights = """
            ALTER TABLE flights_old ADD COLUMN Traj geometry;
        """.trimIndent()

        val alterTraj = """
            UPDATE flights_old SET Traj = trajectory(trip);
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
            SELECT COUNT(*) FROM flights_old;
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
fun loadData(){

    try {

        val createTable = """
            CREATE TABLE flightInput(
                ECTRLNumber integer, 
                SequenceNumber integer, 
                timestamp Timestamp, 
                flightLevel float, 
                latitude float, 
                longitude float,
                Geom geometry(Point, 4326)
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
            SELECT create_distributed_table('flightinput', 'ectrlnumber');
            """.trimIndent()

        val distributeQueryResult = statement.executeQuery(distributeTables)

        println("Distributed Table flightInput with ectrlnumber as sharding key.")

    }catch (e: Exception) {
        println("Could not distribute table flightInput.")
        e.printStackTrace()

    }

    }

    try {
        val setDateStyle = """
            SET DateStyle = 'DMY';
        """.trimIndent()

        statement.executeUpdate(setDateStyle)

        val copyDataFromCsvFile = """
            COPY flightInput
            (ECTRLNumber, 
             SequenceNumber, 
             timestamp, 
             flightLevel, 
             latitude, 
             longitude) FROM '/home/felix/FlightData.csv' DELIMITER ',' CSV HEADER;
        """.trimIndent()

        println("Copying flight data from csv file into the database ...")

        println("Number of added Records: " + statement.executeUpdate(copyDataFromCsvFile))
    }
    catch (e: Exception) {
        println("Could not load data from csv file into flightInput table.")
    }

    try {
        val updateGeoms = """
            UPDATE flightInput SET Geom = ST_SetSRID( ST_MakePoint( Longitude, Latitude ), 4326);
        """.trimIndent()

        println("Updating database to hold geometry points made of latitude and longitude.")
        println("Number of changed rows: " + statement.executeUpdate(updateGeoms))

    }catch (e: Exception){
        println("Could not update Geometry Points.")
    }

}