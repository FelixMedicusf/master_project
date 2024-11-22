import com.fasterxml.jackson.annotation.JsonProperty

data class QueryConfiguration(val name: String, // name of the query
                              val type: String, // type of the query (temporal, spatial, or spatiotemporal
                              val sql: String, // the sql statement to perform
                              val repetition: Int, // how often should each query with each parameter be executed
                              @JsonProperty("param_sets") val paramSets: List<Map<String, Map<String, Any>>>? = null, // a list of pairs (key, value) of
                              val use: Boolean)// parameters to be used in the queries
