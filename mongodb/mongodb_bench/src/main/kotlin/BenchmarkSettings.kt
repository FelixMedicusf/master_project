import com.fasterxml.jackson.annotation.JsonProperty

data class BenchmarkSettings(
    val sut: String,
    val mixed: Boolean,
    val nodes: List<String>,
    val threads: Int,
    val test: Boolean,
    @JsonProperty("random_seed")val randomSeed: Long
)