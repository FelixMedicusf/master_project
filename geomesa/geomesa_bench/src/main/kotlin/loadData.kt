import org.geotools.api.data.DataStoreFinder.getDataStore
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams


fun main(){

    val managerNode = "34.140.7.207"
    val userName = "root"
    val password = "master"
    val catalog = "flightcatalog"

    val flightId = "69491084"
    val points = "flightpoints"
    val trips = "flighttrips"

    val typeName = "counties"



    val connectionParams = mapOf(
        AccumuloDataStoreParams.InstanceNameParam().key to "accumulo-node-1",
        AccumuloDataStoreParams.ZookeepersParam().key to "$managerNode:2181",
        AccumuloDataStoreParams.UserParam().key to "$userName",
        AccumuloDataStoreParams.PasswordParam().key to "$password",
        AccumuloDataStoreParams.CatalogParam().key to "$catalog"
    )
    // val dataStore = getDataStore(connectiondarams)

    val dataStore = getDataStore(connectionParams)
    val countiesTable = dataStore.getFeatureSource(typeName)
    val citiesTable = dataStore.getFeatureSource("cities")
    val flightPointsTypeName = "flightpoints"

    // Retrieve the `flightpoints` feature source
    val flightPointsTable = dataStore.getFeatureSource(flightPointsTypeName)
    println("Connected to the datastore: $dataStore")

    try {



    }catch (e: Exception){
        println("Could not connect to Accumulo DataStore!")

    }

    try {
        val countyName = "Krs Duisburg"
        val cqlFilter = ECQL.toFilter("name = '$countyName'")
        var query = Query(typeName, cqlFilter)

        val queryResponse = countiesTable.getFeatures(query)


        queryResponse.features().use { features ->
            while (features.hasNext()) {
                val feature: SimpleFeature = features.next()
                val geometry = feature.defaultGeometry
                println("Retrieved geometry: $geometry")

                val inCountyFilter = ECQL.toFilter("INTERSECTS(geom, ${geometry})")

                query = Query(flightPointsTypeName, inCountyFilter, "flightId")
                val inCountyQuery = flightPointsTable.getFeatures(query)

                inCountyQuery.features().use {
                    features ->
                    while (features.hasNext()){
                        val feature: SimpleFeature = features.next()
                        println(feature.getAttribute(0))
                    }
                }

            }

        }

        val distanceQuery = Query(
            "cities",
            ECQL.toFilter(
                """
            name = 'Aachen' OR name = 'Cologne'
            """
            ),
          "name", "geom")


        // Execute the query to get the geometries of the two cities
        val featureCollection = citiesTable.getFeatures(distanceQuery)

        val geometries = mutableListOf<org.locationtech.jts.geom.Geometry>()

        // Collect geometries
        featureCollection.features().use { features ->
            while (features.hasNext()) {
                val feature: SimpleFeature = features.next()
                println(feature.getAttribute("geom"))

                val geom = feature.getDefaultGeometry() as org.locationtech.jts.geom.Geometry
                geometries.add(geom)
            }
        }
        val city1 = geometries[0]
        val city2 = geometries[1]

        val computedDistanceQuery = Query(
            "cities",
            ECQL.toFilter("distance(${city1.toText()}, ${city2.toText()}) >= 0"), // Placeholder query
            "distance(${city1.toText()}, ${city2.toText()}) AS distance")




    }catch (e: Exception){
        e.printStackTrace()
    }
}