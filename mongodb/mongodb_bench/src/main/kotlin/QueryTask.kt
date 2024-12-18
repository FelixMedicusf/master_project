data class QueryTask(
    val queryName: String,
    val type: String,
    val mongoQuery: String,
    val params: List<String> ?= null,

)
