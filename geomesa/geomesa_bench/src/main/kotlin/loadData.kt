package geomesa

import org.geotools.api.data.DataStoreFinder.getDataStore
import org.geotools.api.filter.Filter
import org.geotools.filter.text.cql2.CQL
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams


fun main(){

    val managerNode = ""
    val userName = "root"
    val password = "master"
    val catalog = "flightcatalog"

    val flightId = "69491084"
    val points = "flightpoints"
    val trips = "flighttrips"

    val connectionParams = mapOf(
        AccumuloDataStoreParams.InstanceNameParam().key to "accumulo-node-1",
        AccumuloDataStoreParams.ZookeepersParam().key to "$managerNode:2181",
        AccumuloDataStoreParams.UserParam().key to "$userName",
        AccumuloDataStoreParams.PasswordParam().key to "$password",
        AccumuloDataStoreParams.CatalogParam().key to "$catalog"
    )
    // val dataStore = getDataStore(connectionParams)

    try {
        val pointsDataStore = getDataStore(connectionParams)
        println("Connected to the datastore: $pointsDataStore")

        for (typeName in pointsDataStore.typeNames) {
            println("Found Feature: $typeName")

        }

        var pointsSource = pointsDataStore.getFeatureSource(points)
        var tripsSource = pointsDataStore.getFeatureSource(trips)

        var pointsSchema = pointsSource.schema
        var tripsSchema = tripsSource.schema

        println()
        println("This is the Schema for the FlightPoints: ")
        println()
        for (descriptor in pointsSchema.attributeDescriptors) {
            println(descriptor.localName + " - " + descriptor.type.binding)
        }

        println()
        println("This is the Schema for the FlightTrips: ")
        println()
        for (descriptor in tripsSchema.attributeDescriptors) {
            println(descriptor.localName + " - " + descriptor.type.binding)
        }

        var filter: Filter? = null

        try {
            filter = CQL.toFilter("flightId=69491084")
        }catch (e: Exception){
            e.printStackTrace()
            println("Failed to set filter.")
        }


        var filteredPoints = pointsSource.getFeatures(filter)
        var filteredTrips = tripsSource.getFeatures(filter)

        var it = filteredPoints.features()

        println("Filtered Flightpoints: \n")
        while (it.hasNext()) {
            val feature = it.next()
            println("${feature.getAttribute("flightId")}, ${feature.getAttribute("timestamp")}, ${feature.getAttribute("airplaneType")}, ${feature.getAttribute("origin")}, ${feature.getAttribute("destination")}, ${feature.getAttribute("geom")}, ${feature.getAttribute("altitude")}")
        }
        println()
        it.close()

        it = filteredTrips.features()

        println("Filtered Flighttrips: \n")
        while (it.hasNext()) {
            val feature = it.next()
            println("${feature.getAttribute(0)}, ${feature.getAttribute(1)}, ${feature.getAttribute(2)}, ${feature.getAttribute(3)}, ${feature.getAttribute(4)}, ${feature.getAttribute(5)}, ${feature.getAttribute(6)}")
        }
        println()
        it.close()

    }catch (e: Exception){
        println("Could not connect to Accumulo DataStore!")
    }
}