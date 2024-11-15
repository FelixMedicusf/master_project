import java.sql.DriverManager

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


        println("Execution of Thread ${Thread.currentThread().id} started.")

        val countQuery = """
            SELECT COUNT(*) FROM flights;
        """.trimIndent()

        var response = statement.executeQuery(countQuery)

        while (response.next()) {
            println(response.getInt(1))
        }

        // getting the closest distance an airplane ever has been to the city of Aachen
        val minDistanceToCity = """
            SELECT c.name, f.flightid, MIN(ST_DistanceSphere(trajectory(f.trip), c.Geom)) AS MinDistance
            FROM citydata c, flights f
            WHERE c.name = 'Aachen'
            GROUP BY c.name, f.flightid
            ORDER BY MinDistance
            LIMIT 10;
            """.trimIndent()

        response = statement.executeQuery(minDistanceToCity)

        while (response.next()) {
            val city = response.getString("name")
            val flightId = response.getString("flightid")
            val minDistance = response.getDouble("MinDistance")
            println("City: $city, Flight ID: $flightId, Minimum Distance: ${minDistance/1000} km")
        }


        val avgSpeed = """
            SELECT MIN(twavg(speed(trip))) * 3.6, MAX(twavg(speed(trip))) * 3.6,
              AVG(twavg(speed(trip))) * 3.6
            FROM flights;
        """.trimIndent()
        response = statement.executeQuery(avgSpeed)

        while (response.next()){
            println("${response.getInt(1)}, ${response.getInt(2)}, ${response.getInt(3)}")
        }

        response.close()
        statement.close()
        connection.close()
    }
}


