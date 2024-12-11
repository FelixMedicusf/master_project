package EuroControlData

import com.opencsv.CSVReader
import com.opencsv.CSVWriter
import java.io.FileReader
import java.io.FileWriter



fun preprocessFlightData() {
    val inputCsvFile = "src/main/resources/Flight_Points_Actual_20220601_20220630.csv"
    val outputCsvFile = "src/main/resources/FlightData.csv"


    val reader = CSVReader(FileReader(inputCsvFile))
    val writer = CSVWriter(FileWriter(outputCsvFile))


    val headers = reader.readNext()

    if (headers != null) {
        writer.writeNext(headers) // Write headers to the output file
    }
    var prevEctrlNumber = ""
    var prevTimestamp = ""

    try {
        // Remove entries where no longitude or latitude is given or entries for the same plane with the same timestamp
        var record: Array<String>? = reader.readNext()
        while (record != null) {
            var currentEctrlNumber = record[0]
            var currentTimeStamp = record[2]

            var samePlaneAndTimeStamp = (currentEctrlNumber == prevEctrlNumber && currentTimeStamp == prevTimestamp)
            if (record[4].isNotEmpty() && record[5].isNotEmpty() && !samePlaneAndTimeStamp) {
                writer.writeNext(record)
            }

            prevEctrlNumber = record[0]
            prevTimestamp = record[2]
            record = reader.readNext()
        }
    } finally {
        reader.close()
        writer.close()
    }
}

fun main(){

    val env = Runtime.getRuntime()
    print(env.maxMemory())
    preprocessFlightData()
}
