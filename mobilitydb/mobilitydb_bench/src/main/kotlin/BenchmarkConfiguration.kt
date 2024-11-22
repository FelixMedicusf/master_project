import com.fasterxml.jackson.annotation.JsonProperty

data class BenchmarkConfiguration(
    @JsonProperty("benchmark") val benchmarkSettings: BenchmarkSettings,
    val queryConfigs: List<QueryConfiguration>
)