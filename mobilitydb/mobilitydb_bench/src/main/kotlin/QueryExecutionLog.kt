data class QueryExecutionLog(
    val threadName: String,
    val queryName: String,
    val queryType: String,
    val paramValues: String,
    val startTimeFirst: Long,
    val endTimeFirst: Long,
    val startTimeSecond: Long,
    val endTimeSecond: Long,
    val latency: Long,
) {

    override fun toString(): String {

        return listOf(
            threadName,
            queryName,
            queryType,
            paramValues,
            startTimeFirst,
            endTimeFirst,
            startTimeSecond,
            endTimeSecond,
            latency,
        ).joinToString(",")
    }
}