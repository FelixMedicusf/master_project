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



    }catch (e: Exception){
        println("Could not connect to Accumulo DataStore!")

        val countiesSource: SimpleFeatureSource = countiesDataStore.getFeatureSource("counties")
        val flightpointsSource: SimpleFeatureSource = flightpointsDataStore.getFeatureSource("flightpoints")
    }
}