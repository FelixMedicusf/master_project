data class QueryExecutionLog(
    val threadName: String,
    val queryName: String,
    val queryType: String,
    val params: Map<String, Any>?,
    val round: Int,
    val executionIndex: Int,
    val startTime: Long,
    val endTime: Long,
    val latency: Long
) {


    override fun toString(): String {

        val paramsString = params?.entries?.joinToString(";") { "${it.key}=${it.value}" } ?: ""

        return listOf(
            threadName,
            queryName,
            queryType,
            paramsString,
            round,
            executionIndex,
            startTime,
            endTime,
            latency
        ).joinToString(",")
    }
}