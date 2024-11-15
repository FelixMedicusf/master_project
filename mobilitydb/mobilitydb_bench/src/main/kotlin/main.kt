import java.util.concurrent.Executors

fun main(){
    val benchThreads = Executors.newFixedThreadPool(4)
    val mobilityDBIp = "34.38.153.212"


    val queryThread = BenchThread(mobilityDBIp, "aviation_data", "felix", "master" )


    benchThreads.submit(queryThread)

    benchThreads.shutdown()


}