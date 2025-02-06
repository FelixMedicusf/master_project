data class QueryTask(
    val queryName: String,
    val type: String,
    val parsedSql: String,
    val paramValues: List<String>
)
