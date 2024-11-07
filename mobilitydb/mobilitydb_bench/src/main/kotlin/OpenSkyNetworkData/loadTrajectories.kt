package OpenSkyNetworkData
import java.sql.DriverManager

val distributedDatabase = true

val mobilityDBIp = ""

val connectionString = "jdbc:postgresql://$mobilityDBIp:5432/aviation_data"
val user = "felix"
val password = "master"

val connection = DriverManager.getConnection(connectionString, user, password)
val statement = connection.createStatement()

fun main(){
    loadData()
    buildTrajectoryData()
}

fun buildTrajectoryData () {

    try{
        // create index on icao24 to make read ops quicker
        val createIndex = """
            CREATE INDEX idx_flightInput_icao24_timestamp ON flightInput (icao24);
            """.trimIndent()

        println("Create an index for flightinput table on the column icao24 and timestamp.")
        // statement.executeUpdate(createIndex)

    }catch (e: Exception){
        e.printStackTrace()
        println("Could not create index on icao24")
    }

    try{
        val createGeomSeq = """
            CREATE TABLE flights(icao24, trip, velocity, heading, vertrate, callsign, onground, alert, spi, squawk
            , baroaltitude) AS
            SELECT icao24, 
            tgeompointSeq(array_agg(tgeompoint( ST_Transform(Geom, 4326), timestamp) ORDER BY timestamp)),
            tfloatSeq(array_agg(tfloat(velocity, timestamp) ORDER BY timestamp) FILTER (WHERE velocity IS NOT NULL)), 
            tfloatSeq(array_agg(tfloat(heading, timestamp) ORDER BY timestamp) FILTER (WHERE heading IS NOT NULL)), 
            tfloatSeq(array_agg(tfloat(vertrate, timestamp) ORDER BY timestamp) FILTER (WHERE vertrate IS NOT NULL)), 
            ttextSeq(array_agg(ttext(callsign, timestamp) ORDER BY timestamp) FILTER (WHERE callsign IS NOT NULL)), 
            tboolSeq(array_agg(tbool(onground, timestamp) ORDER BY timestamp) FILTER (WHERE onground IS NOT NULL)), 
            tboolSeq(array_agg(tbool(alert, timestamp) ORDER BY timestamp) FILTER (WHERE alert IS NOT NULL)), 
            tboolSeq(array_agg(tbool(spi, timestamp) ORDER BY timestamp) FILTER (WHERE spi IS NOT NULL)), 
            tintSeq(array_agg(tint(squawk, timestamp) ORDER BY timestamp) FILTER (WHERE squawk IS NOT NULL)), 
            tfloatSeq(array_agg(tfloat(baroaltitude, timestamp) ORDER BY timestamp) FILTER (WHERE baroaltitude IS NOT NULL))
            FROM flightInput 
            GROUP BY icao24;
        """.trimIndent()

        // tfloatSeq(array_agg(tfloat(geoaltitude, timestamp) ORDER BY timestamp) FILTER (WHERE geoaltitude IS NOT NULL))
        // ttextSeq(array_agg(ttext(lastposupdate, timestamp) ORDER BY timestamp) FILTER (WHERE lastposupdate IS NOT NULL)),
        // ttextSeq(array_agg(ttext(lastcontact, timestamp) ORDER BY timestamp) FILTER (WHERE lastcontact IS NOT NULL))

        println("Create Table flights to build trajectories for flight data.")
        statement.executeUpdate(createGeomSeq)

    }catch(e: Exception){

        println("Could not create table for trajectories.")
        e.printStackTrace()
    }

    try {
        val addTrajColumn = """
            ALTER TABLE flights ADD COLUMN Traj geometry;
        """.trimIndent()

        val createTrajs = """
            UPDATE flights SET Traj = trajectory(trip);
        """.trimIndent()

        println("Create column for trajectories in flights table.")
        statement.executeUpdate(addTrajColumn)
        statement.executeUpdate(createTrajs)

    }catch (e: Exception){
        println("Could not create trajectories out of geometry points senquences.")
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

        println("Flight Table now contains Trajectories for ${String.format("%,d", numberOfRows)} flights.")

    }catch (e: Exception){
        println("Could not execute COUNT query.")
        e.printStackTrace()
    }
}
fun loadData(){

    try {

        val createTable = """
            CREATE TABLE flightInput(
                timestamp TIMESTAMP, 
                icao24 varchar(20), 
                latitude float, 
                longitude float, 
                velocity float, 
                heading float,
                vertrate float, 
                callsign varchar(10), 
                onground boolean, 
                alert boolean, 
                spi boolean, 
                squawk integer, 
                baroaltitude numeric(7,2),
                Geom geometry(Point, 4326)
        )
        """.trimIndent()

        /*       geoaltitude numeric(7,2),
                lastposupdate TIMESTAMP,
                lastcontact TIMESTAMP,
         */

        val tablesAffected = statement.executeUpdate(createTable)
        println("Created Table flightInput.")

    }catch (e: Exception){
        println("Could not create Table flightInput.")
        e.printStackTrace()

    }


    if (distributedDatabase) {
    try {

        val distributeTables = """
            SELECT create_distributed_table('flightinput', 'icao24');
            """.trimIndent()
        println("Distributed Table flightInput with icao24 as sharding key.")
        val distributeQueryResult = statement.executeQuery(distributeTables)



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
            (timestamp, 
             icao24, 
             latitude, 
             longitude, 
             velocity, 
             heading, 
             vertrate, 
             callsign, 
             onground, 
             alert, 
             spi, 
             squawk, 
             baroaltitude 
           ) FROM '/home/felix/FlightData.csv' DELIMITER ',' CSV HEADER NULL '';
        """.trimIndent()

        /*
                     geoaltitude,
             lastposupdate,
             lastcontact
         */

        println("Copying flight data from csv file into the database ...")

        println("Number of added Records: " + String.format("%,d", statement.executeUpdate(copyDataFromCsvFile)))
    }
    catch (e: Exception) {
        e.printStackTrace()
        println("Could not load data from csv file into flightInput table.")
    }
    /*
    try {
        val updateEmptyColumns = """
            UPDATE flightinput
            SET
                squawk = NULLIF(squawk, ''),
                velocity = NULLIF(velocity, ''),
                heading = NULLIF(heading, ''),
                vertrate = NULLIF(vertrate, ''),
                baroaltitude = NULLIF(baroaltitude, ''),
                geoaltitude = NULLIF(geoaltitude, ''),
                lastposupdate = NULLIF(lastposupdate, ''),
                lastcontact = NULLIF(lastcontact, '');
                
        """.trimIndent()

        println("Change columns to hold null values for empty strings.")
        statement.executeUpdate(updateEmptyColumns)



    }catch (e: Exception){
        println("Could not update columns.")
        e.printStackTrace()
    }

    try {
        val updateColumnTypes = """
            ALTER TABLE flightinput
            ALTER COLUMN squawk TYPE INTEGER USING squawk::INTEGER,
            ALTER COLUMN velocity TYPE float USING velocity::FLOAT,
            ALTER COLUMN heading TYPE float USING heading::FLOAT,
            ALTER COLUMN vertrate TYPE float USING vertrate::FLOAT,
            ALTER COLUMN baroaltitude TYPE numeric(7,2) USING baroaltitude::NUMERIC(7,2),
            ALTER COLUMN geoaltitude TYPE numeric(7,2) USING geoaltitude::NUMERIC(7,2),
            ALTER COLUMN lastposupdate TYPE numeric(13,3) USING lastposupdate::NUMERIC(13,3),
            ALTER COLUMN lastcontact TYPE numeric(13,3) USING lastcontact::NUMERIC(13,3);
        """.trimIndent()

        println("Change data types of columns.")
        statement.executeUpdate(updateColumnTypes)



    }catch (e: Exception){
        println("Could not update columns.")
        e.printStackTrace()
    }



    try{
        val addTSColumns = """
            ALTER TABLE flightinput
                ADD COLUMN et_ts timestamp,
                ADD COLUMN lastposupdate_ts timestamp,
                ADD COLUMN lastcontact_ts timestamp;
        """.trimIndent()

        statement.executeUpdate(addTSColumns)

        val setTimestamps = """
        UPDATE flightinput
            SET et_ts = to_timestamp(timestamp),
                lastposupdate_ts = to_timestamp(lastposupdate),
                lastcontact_ts = to_timestamp(lastcontact);
        """.trimIndent()

        statement.executeUpdate(setTimestamps)

    }catch (e: Exception){
        println("Could not add timestamps")
        e.printStackTrace()

    }
     */

    try {
        val updateGeoms = """
            UPDATE flightInput SET Geom = ST_SetSRID( ST_MakePoint( longitude, latitude ), 4326);
        """.trimIndent()

        println("Updating database to hold geometry points made of latitude and longitude.")
        println("Number of changed rows: " + String.format("%,d", statement.executeUpdate(updateGeoms)))

    }catch (e: Exception){
        println("Could not update Geometry Points.")
    }

}

// Determines how much memory PostgreSQL uses for caching data.
// 25% of RAM is Recommendation
// shared buffers = 2GB

// specifies the amount of memory to be used by internal sort ops and hash tables before writing to temporary disk files
// (take care: value is per  op)
// work_mem = 64MB

// Used for maintenance operations like CREATE INDEX, VACUUM, and ALTER TABLE.
// maintenance_work_mem = 512MB

// Controls the number of workers that can be started by a single Gather or Gather Merge node.
// max_parallel_workers_per_gather = 4

// These settings control the total number of background processes and parallel workers.
// max_worker_processes = 16
// max_parallel_workers = 16

// Enhances the performance of parallel scans by increasing the number of concurrent disk operations.
// effective_io_concurrency = 200

// Controls the amount of WAL (Write-Ahead Logging) data. Increase max_wal_size to reduce the frequency of checkpoints, which can improve write performance.
// max_wal_size = 2GB
// min_wal_size = 1GB

// Controls the maximum number of parallel workers for a single query across all workers.
// SET citus.max_parallel_workers_per_query = 4;

// Determines the number of concurrent tasks that can be scheduled.
// citus.task_scheduler_slots = 8;

// max_parallel_maintenance_workers = 2


// OS tuning
// sysctl -w kernel.shmmax=2147483648
// sysctl -w kernel.shmall=524288
// ulimit -n 10000

// Monitor active queries and their performance.
// SELECT * FROM pg_stat_activity;

// Analyze query execution plans to identify bottlenecks.
// EXPLAIN ANALYZE CREATE INDEX CONCURRENTLY idx_flightInput_icao24_timestamp ON flightInput (icao24, timestamp);



//SET enable_hashagg = on;
//SET enable_sort = off;
// SET work_mem = '512MB';


// SET max_parallel_workers_per_gather = 4;  -- Adjust this based on CPU cores
// SET parallel_tuple_cost = 0.1;             -- Encourage parallelism
// SET parallel_setup_cost = 100;