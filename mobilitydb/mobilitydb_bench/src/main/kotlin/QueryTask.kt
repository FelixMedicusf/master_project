data class QueryTask(
    val queryName: String,
    val type: String,
    val sql: String,
    val params: Map<String, Any> ?= null,
    val use: Boolean,
)
