import com.fasterxml.jackson.annotation.JsonProperty

data class QueryConfiguration(
    val name: String, // name of the query
    val use: Boolean, // whether the query is enabled
    val type: String, // type of the query (temporal, spatial, or spatiotemporal)
    val repetition: Int, // how often each query with each parameter is executed
    @JsonProperty("parameter_sets")
    val parameters: List<String>? = null // list of parameter sets with their values
)

data class QueryParameterSet(
    val parameters: Map<String, String> // key-value pairs for parameter names and values
)