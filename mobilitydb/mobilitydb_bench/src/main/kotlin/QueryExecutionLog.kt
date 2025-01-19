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
    val startTimeSecond: Long?,
    val endTimeSecond: Long?,
    val latency: Long,
    val records: Int
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
            startTimeSecond,
            endTimeSecond,
            latency,
            records
        ).joinToString(",")
    }
}