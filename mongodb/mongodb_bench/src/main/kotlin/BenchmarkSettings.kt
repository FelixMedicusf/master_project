import com.fasterxml.jackson.annotation.JsonProperty

data class BenchmarkSettings(
    val sut: String,
    val nodes: List<String>,
    val threads: Int,
    @JsonProperty("random_seed")val randomSeed: Long
)