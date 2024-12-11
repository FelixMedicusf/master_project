package OpenSkyNetworkData

import com.opencsv.CSVReader
import com.opencsv.CSVWriter
import java.io.File
import java.io.FileReader
import java.io.FileWriter
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter

fun preprocessFlightData() {
    val inputDirectory = "src/main/resources/OpenSkyNetworkData"
    val outputCsvFile = "src/main/resources/OpenSkyNetworkData/output/FlightData.csv"

    val directory = File(inputDirectory)

    if (directory.exists() && directory.isDirectory) {

        val csvFiles = directory.listFiles { file -> file.extension == "csv" }

        val writer = CSVWriter(FileWriter(outputCsvFile))

        var count = 1;
        csvFiles?.forEach { currentCsvFile ->

            println("Processing file: ${currentCsvFile.name}.")
            val reader = CSVReader(FileReader(currentCsvFile))

            var header = reader.readNext()

            if (header != null && count==1) {
                header = header.filterIndexed { index, _ -> index != 13 && index != 14 && index != 15}.toTypedArray()
                writer.writeNext(header) // Write headers to the output file
            }

            count++;

            var prevEctrlNumber = ""
            var prevTimestamp = ""

            try {
                // Remove entries where no longitude or latitude is given or entries for the same plane with the same timestamp
                var record: Array<String>? = reader.readNext()
                while (record != null) {

                    var timestamp = record[0] //bigint
                    var icao24 = record[1] //varchar
                    var latitude = record[2] //float
                    var longitude = record[3] //float
                    var velocity = record[4] //float
                    var heading = record[5] //float
                    var vertrate = record[6] //float
                    var callsign = record[7] //varchar
                    var onground = record[8] //boolean
                    var alert = record[9] //boolean
                    var spi = record[10] //boolean
                    var squawk = record[11] //integer
                    var baroaltitude = record[12] //numeric
                    var geoaltitude = record[13] //numeric
                    var lastposupdate = record[14] //numeric
                    var lastcontact = record[15] //numeric

                    record[7] = record[7].trimEnd()

                    if(velocity.isEmpty())record[4]="-1000.0"
                    if(heading.isEmpty())record[5]="-1000.0"
                    if(vertrate.isEmpty())record[6]="-1000.0"
                    if(squawk.isEmpty())record[11]="-1000"
                    if(baroaltitude.isEmpty())record[12]="-1000.0"
                    //if(geoaltitude.isEmpty())record[13]="-1000.0"

                    record = record.filterIndexed { index, _ -> index != 13 && index != 14 && index != 15}.toTypedArray()

                    if (latitude.isNotEmpty() && longitude.isNotEmpty()) {

                        record[0] = unixTimestampToDMYHMS(timestamp)
                    //record[14] = unixTimestampToDMYHMS(lastposupdate.split(".")[0])
                    //record[15] = unixTimestampToDMYHMS(lastcontact.split(".")[0])
                        writer.writeNext(record)
                    }

                    record = reader.readNext()
                }
            } finally {
                reader.close()
            }

        }

        writer.close()

    } else {
        println("Directory does not exist or is not a directory")
    }


}

fun unixTimestampToDMYHMS(unixTimestamp: String): String {

    val instant = Instant.ofEpochSecond(unixTimestamp.toLong())

    val dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault())

    val formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss")

    return dateTime.format(formatter)
}

fun main(){

    // val env = Runtime.getRuntime()
    // print(env.maxMemory())
    preprocessFlightData()
}
