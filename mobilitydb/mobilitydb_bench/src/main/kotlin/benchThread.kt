import java.sql.DriverManager
import java.sql.Statement

class BenchThread(
    mobilityDBIp: String,
    databaseName: String,
    user: String,
    password: String

    ) : Thread() {


    val connectionString = "jdbc:postgresql://$mobilityDBIp:5432/$databaseName"
    val connection = DriverManager.getConnection(connectionString, user, password)
    val statement = connection.createStatement()


    override fun run() {


        println("Execution of Thread ${currentThread().id} started.")

        val countQuery = """
            SELECT COUNT(*) FROM flights;
        """.trimIndent()

        var response = statement.executeQuery(countQuery)

        while (response.next()) {
            println(response.getInt(1))
        }


        closePlanes(statement, 100000)


        response.close()
        statement.close()
        connection.close()
    }

    // spatiotemporal query
    // list the 10 closest flights to a city from cityData during a time period from periods
    fun minDistanceQuery (statement: Statement, city: String, timePeriod: String? = null) {

        var minDistanceQuery = ""

        if (timePeriod == null) {
            minDistanceQuery = """
            SELECT c.name, f.flightid, MIN(ST_DistanceSphere(trajectory(f.trip), c.Geom)) AS MinDistance
            FROM citydata c, flights f
            WHERE c.name = '$city'
            GROUP BY c.name, f.flightid
            ORDER BY MinDistance
            LIMIT 10;
            """.trimIndent()
        }else{
            minDistanceQuery = """
            SELECT c.name, f.flightid, MIN(ST_DistanceSphere(trajectory(f.trip), c.Geom)) AS MinDistance
            FROM citydata c, flights f
            WHERE c.name = '$city'
            GROUP BY c.name, f.flightid
            ORDER BY MinDistance
            LIMIT 10;
            """.trimIndent()
        }

        var response = statement.executeQuery(minDistanceQuery)


        while (response.next()) {
            val city = response.getString("name")
            val flightId = response.getString("flightid")
            val minDistance = response.getDouble("MinDistance")
            println("City: $city, Flight ID: $flightId, Minimum Distance: ${minDistance/1000} km")
        }
        response.close()

    }


    // Spationtemporal join
    // Airplanes that come closer than X meters to one another
    fun closePlanes(statement: Statement, distance: Int){

        var closePlanesQuery = """
            SELECT P1.flightid, P2.flightid, P1.traj, P2.traj,
            shortestLine(P1.trip, P2.trip) as Approach
            FROM flights P1, flights P2
            WHERE P1.flightid > P2.flightid AND
            adwithin(P1.trip, P2.trip, 100000)
        """.trimIndent()

        var response = statement.executeQuery(closePlanesQuery)

        while(response.next()){
            println("${response.getInt(1)}, ${response.getInt(2)}, ${response.getInt(3)}")
        }

        response.close()
    }

    fun speedMetrics (statement: Statement){

        val avgSpeed = """
            SELECT MIN(twavg(speed(trip))) * 3.6, MAX(twavg(speed(trip))) * 3.6,
              AVG(twavg(speed(trip))) * 3.6
            FROM flights;
        """.trimIndent()

        var response = statement.executeQuery(avgSpeed)

        while (response.next()){
            println("${response.getInt(1)}, ${response.getInt(2)}, ${response.getInt(3)}, " +
                    "${response.getInt(4)}, ${response.getInt(5)}")
        }
        response.close()
    }
}






