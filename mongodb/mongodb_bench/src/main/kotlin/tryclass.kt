import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import org.bson.Document
import java.io.File
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import kotlin.random.Random
import kotlin.reflect.KFunction
import kotlin.reflect.full.declaredFunctions
import kotlin.reflect.typeOf

class tryclass {

    val random = Random(123)
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    private val municipalitiesPath = "src/main/resources/municipalities.csv"
    private val countiesPath = "src/main/resources/counties.csv"
    private val districtsPath = "src/main/resources/districts.csv"
    private val citiesPath = "src/main/resources/cities.csv"
    private val airportsPath = "src/main/resources/airports.csv"
    private val airplanetypesPath = "src/main/resources/airplanetypes.csv"

    private val municipalities = parseCSV(municipalitiesPath, setOf("name"))
    private val counties = parseCSV(countiesPath, setOf("name"))
    private val districts = parseCSV(districtsPath, setOf("name"))
    private val cities = parseCSV(citiesPath, setOf("area", "lat", "lon", "district", "name", "population"))
    private val airports = parseCSV(airportsPath, setOf("IATA", "ICAO", "Airport name", "Country", "City"))
    private val airplanetypes = File(airplanetypesPath).readLines()


    fun returnParamValues(params: List<String>): List<Any> {
        var values = ArrayList<Any>()
        for (param in params){
            val replacement = when (param) {
                "period_short" -> generateRandomTimeSpan(random=this.random, formatter = this.formatter, year=2023, mode=1)
                "period_medium" -> generateRandomTimeSpan(random=this.random, formatter = this.formatter, year=2023, mode=2)
                "period_long" -> generateRandomTimeSpan(random=this.random, formatter = this.formatter, year=2023, mode=3)
                "period" -> generateRandomTimeSpan(formatter = this.formatter, year=2023, random = this.random)
                "instant" -> generateRandomTimestamp(formatter = this.formatter, random = this.random)
                "day" -> getRandomDay(random = this.random, year = 2023)
                "city" -> getRandomPlace(this.cities, "name", this.random)
                "airport" -> getRandomPlace(this.airports, "Airport name", this.random)
                "municipality" -> getRandomPlace(this.municipalities, "name", this.random)
                "county" -> getRandomPlace(this.counties, "name", this.random)
                "district" -> getRandomPlace(this.districts, "name", this.random)
                "point" -> getRandomPoint(this.random, listOf(listOf(6.212909, 52.241256), listOf(8.752841, 50.53438)))
                "radius" -> (random.nextInt(100, 200) * 10).toString();
                "low_altitude" -> (random.nextInt(300, 600) * 10).toString();
                "type" -> "${airplanetypes[random.nextInt(0, airplanetypes.size)]}"
                else -> ""

            }
            if (replacement != null) {
                values.add(replacement)
            }

        }
        return values
    }

    private fun printMongoResponse(response: Any?) {
        // Configure Jackson ObjectMapper for pretty JSON formatting
        val mapper = ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)

        var castedResponse  = response as List<Document>
        println("MongoDB Response:")
        castedResponse.forEach { document ->
            try {
                // Convert the Document to a JSON string and print it
                val jsonString = mapper.writeValueAsString(document.toMap())
                println(jsonString)
            } catch (e: Exception) {
                println("Error formatting document: ${document.toJson()}")
            }
        }
    }

    private fun generateRandomTimestamp(random: Random, formatter: DateTimeFormatter): String {
        val year = 2023
        val dayOfYear = random.nextInt(1, 366) // Days in the year 2023
        val hour = random.nextInt(0, 24)
        val minute = random.nextInt(0, 60)
        val second = random.nextInt(0, 60)

        // Use LocalDate.ofYearDay to get the date and then add the time
        val date = LocalDate.ofYearDay(year, dayOfYear)
        val timestamp = LocalDateTime.of(date, java.time.LocalTime.of(hour, minute, second))

        return timestamp.format(formatter)
    }

    // Function to generate a random time span (period) within 2023
    fun generateRandomTimeSpan(random: Random, formatter: DateTimeFormatter, year: Int, mode: Int = 0): List<String> {

        // Generate pseudo random timestamps based on the mode
        // 1: for short time range (0-2 days), 2: for medium time range (2 days - 1 month), 3: for long time range (1 - 12 Month)
        // 0: Full random (0 days to 12 months)
        val startDayOfYear = random.nextInt(1, 366)
        val startHour = random.nextInt(0, 24)
        val startMinute = random.nextInt(0, 60)
        val startSecond = random.nextInt(0, 60)

        val date1 = LocalDate.ofYearDay(year, startDayOfYear)
        val timestamp1 = LocalDateTime.of(date1, java.time.LocalTime.of(startHour, startMinute, startSecond))

        // Calculate the end timestamp based on the mode
        val endDate: LocalDateTime = when (mode) {
            1 -> {
                // Up to 2 days
                val daysToAdd = random.nextLong(0, 2 + 1)
                val tentativeEnd = timestamp1.plusDays(daysToAdd)
                if (tentativeEnd.year == year) tentativeEnd else timestamp1.withDayOfYear(365).withHour(23).withMinute(59).withSecond(59)
            }
            2 -> {
                // Between 2 days and 1 month
                val daysToAdd = random.nextLong(2, 30 + 1)
                val tentativeEnd = timestamp1.plusDays(daysToAdd)
                if (tentativeEnd.year == year) tentativeEnd else timestamp1.withDayOfYear(365).withHour(23).withMinute(59).withSecond(59)
            }
            3 -> {
                // Between 1 and 12 months
                val monthsToAdd = random.nextLong(1, 12 + 1)
                val tentativeEnd = timestamp1.plusMonths(monthsToAdd)
                if (tentativeEnd.year == year) tentativeEnd else timestamp1.withDayOfYear(365).withHour(23).withMinute(59).withSecond(59)
            }
            else -> {

                val randomDay = random.nextInt(1, 366)
                val randomHour = random.nextInt(0, 24)
                val randomMinute = random.nextInt(0, 60)
                val randomSecond = random.nextInt(0, 60)

                val date2 = LocalDate.ofYearDay(year, randomDay)
                LocalDateTime.of(date2, java.time.LocalTime.of(randomHour, randomMinute, randomSecond))
            }
        }

        //Ensure start is before end
        val (start, end) = if (timestamp1.isBefore(endDate)) {
            timestamp1 to endDate
        } else {
            endDate to timestamp1
        }

        return listOf(start.format(formatter), end.format(formatter))
    }


    private fun parseCSV(filePath: String, requiredColumns: Set<String>): List<Map<String, String>> {
        val rows = mutableSetOf<Map<String, String>>()
        val lines = File(filePath).readLines()

        if (lines.isNotEmpty()) {
            val header = lines.first().split(",")

            val indicesToKeep = header.withIndex()
                .filter { it.value in requiredColumns }
                .map { it.index }

            rows.addAll(
                lines.drop(1).map { line ->
                    val values = line.split(",")
                    indicesToKeep.associate { header[it] to values[it] }
                }.distinct()
            )
        }

        return rows.toList()
    }

    private fun getRandomPlace(
        parsedData: List<Map<String, String>>,
        columnName: String,
        random: Random
    ): String? {

        val columnValues = parsedData.mapNotNull { it[columnName] }

        if (columnValues.isEmpty()) return null

        return columnValues[random.nextInt(columnValues.size)]
    }

    private fun getRandomPoint(random: Random, rectangle: List<List<Double>>): List<Double> {
        val upperLeftLon = rectangle[0][0]
        val upperLeftLat = rectangle[0][1]
        val bottomRightLon = rectangle[1][0]
        val bottomRightLat = rectangle[1][1]

        val randomLon = upperLeftLon + random.nextDouble() * (bottomRightLon - upperLeftLon)

        val randomLat = bottomRightLat + random.nextDouble() * (upperLeftLat - bottomRightLat)

        // Return the random point
        return listOf(randomLon, randomLat)
    }


    private fun getRandomDay(random: Random, year: Int): String {

        val startDate = LocalDate.of(year, 1, 1)
        val endDate = LocalDate.of(year, 12, 31)

        val daysInYear = endDate.toEpochDay() - startDate.toEpochDay() + 1

        val randomDay = startDate.plusDays(random.nextLong(0, daysInYear))

        return "$randomDay"
    }

    private fun invokeFunctionByName(functionName: String): KFunction<Quadrupel<Long, Long, Long, List<Document>>> {
        val benchThreadClass = this::class

        val function = benchThreadClass.declaredFunctions.find { it.name == functionName }
            ?: throw IllegalArgumentException("Function $functionName not found in ${benchThreadClass.simpleName}")

        // Validate the return type
        if (function.returnType != typeOf<Quadrupel<Long, Long, Long, List<Document>>>()) {
            throw IllegalArgumentException("Function $functionName does not return the expected type")
        }

        @Suppress("UNCHECKED_CAST") // Safe because of the type check above
        return function as KFunction<Quadrupel<Long, Long, Long, List<Document>>>
    }


}
    fun main(){

        val municipalitiesCode = generateListCode(municipalitiesPath, setOf("name"), "municipalities")
        val countiesCode = generateListCode(countiesPath, setOf("name"), "counties")
        val districtsCode = generateListCode(districtsPath, setOf("name"), "districts")
        val citiesCode = generateListCode(citiesPath, setOf("area", "lat", "lon", "district", "name", "population"), "cities")
        val airportsCode = generateListCode(airportsPath, setOf("IATA", "ICAO", "Airport name", "Country", "City"), "airports")

//        println(municipalitiesCode)
//        println(countiesCode)
//        println(districtsCode)
//        println(citiesCode)
//        println(airportsCode)

    }

fun generateListCode(filePath: String, requiredColumns: Set<String>, variableName: String): String {
    val rows = parseCSV(filePath, requiredColumns) // Reuse your parseCSV function

    val listEntries = rows.joinToString(",\n") { row ->
        val mapEntries = row.entries.joinToString(", ") { (key, value) -> "\"$key\" to \"$value\"" }
        "    mapOf($mapEntries)"
    }

    return """
        private val $variableName = listOf(
$listEntries
        )
    """.trimIndent()
}
private val municipalitiesPath = "src/main/resources/municipalities.csv"
private val countiesPath = "src/main/resources/counties.csv"
private val districtsPath = "src/main/resources/districts.csv"
private val citiesPath = "src/main/resources/cities.csv"
private val airportsPath = "src/main/resources/airports.csv"
