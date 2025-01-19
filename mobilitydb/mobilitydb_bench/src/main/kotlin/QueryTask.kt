data class QueryTask(
    val queryName: String,
    val type: String,
    val sql: String,
    val params: List<String> ?= null,
)
