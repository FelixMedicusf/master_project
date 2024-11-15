import org.geotools.api.data.DataStoreFinder
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams

class BenchThread(
    private val instanceName: String,
    private val zookeepers: String,
    private val username: String,
    private val password: String,
    private val catalog: String,
    threadName: String? = null,
    threadPriority: Int = Thread.NORM_PRIORITY,

) : Thread() {

    init {

        this.name = threadName ?: "BenchThread"
        this.priority = threadPriority
    }


    override fun run() {
        println("Thread $name is running with priority $priority.")
        println("Zookeepers: $zookeepers, Username: $username, Catalog: $catalog")

        // Database connection parameters
        val connectionParams = mapOf(
            AccumuloDataStoreParams.InstanceNameParam().key to instanceName,
            AccumuloDataStoreParams.ZookeepersParam().key to "$zookeepers:2181",
            AccumuloDataStoreParams.UserParam().key to username,
            AccumuloDataStoreParams.PasswordParam().key to password,
            AccumuloDataStoreParams.CatalogParam().key to catalog
        )

        // Get DataStore connection
        try {
            val pointsDataStore = DataStoreFinder.getDataStore(connectionParams)
            println("Successfully connected to the database.")

        } catch (e: Exception) {
            println("Failed to connect to the database: ${e.message}")
        }
    }
}


