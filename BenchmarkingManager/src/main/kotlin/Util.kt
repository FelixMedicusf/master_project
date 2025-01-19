import java.io.File
import kotlin.math.ceil


// used for determine the lower and upper bounds of flightIds for the DataHandler for the different coroutines
fun splitFlightDatasetByFlightId(){
    val filePath = "C:\\Users\\Felix Medicus\\Desktop\\Master_Thesis\\master_project\\data\\dfsData\\output\\FlightPointsMobilityDBlarge.csv"

    val totalLines = 166933239

    for (i in 1..< 11) {
        try {
            val lineNumber = (ceil(totalLines.toDouble() / 10 * i)).toInt()
            // Open the file and retrieve the specific line
            val line = File(filePath).useLines { lines ->
                lines.drop(lineNumber - 1).firstOrNull() // Drop lines until the target and get the first remaining
            }

            if (line != null) {
                val numberString = line.split(",")[0].trim('"')
                if(i == 1)print("listOf(0, ")
                if (i != 10)print("$numberString, ")
                if (i == 10)print(numberString)
            } else {
                println("Line $lineNumber does not exist in the file.")
            }

        } catch (e: Exception) {
            println("An error occurred: ${e.message}")
        }
    }

    print(")")

}
fun main(){
    splitFlightDatasetByFlightId()
}