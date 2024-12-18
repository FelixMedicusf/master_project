data class QueryExecutionLog(
    val threadName: String,
    val queryName: String,
    val queryType: String,
    val params: String?,
    val paramValues: String,
    val round: Int,
    val executionIndex: Int,
    val startTime: Long,
    val endTime: Long,
    val latency: Long
) {


    override fun toString(): String {


        return listOf(
            threadName,
            queryName,
            queryType,
            params,
            paramValues,
            round,
            executionIndex,
            startTime,
            endTime,
            latency
        ).joinToString(",")
    }
}