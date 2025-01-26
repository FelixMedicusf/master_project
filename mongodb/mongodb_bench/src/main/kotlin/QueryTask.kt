data class QueryTask(
    val queryName: String,
    val type: String,
    val paramSet: Map<String, String>  ?= null,
)
