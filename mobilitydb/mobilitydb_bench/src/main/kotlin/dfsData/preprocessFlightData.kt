package geomesa.dfsData

import com.opencsv.CSVReader
import com.opencsv.CSVWriter
import java.io.File
import java.io.FileReader
import java.io.FileWriter
import java.time.*
import java.time.format.DateTimeFormatter

fun createFlightPointsMobilityDB(totalNumberRows: Int = -1) {


    val inputDirectory = "../../data/dfsData"
    val outputCsvFile = "../../data/dfsData/output/FlightPointsMobilityDBlarge.csv"

    val directory = File(inputDirectory)
    val formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss")
    var metaData = false

    var newFlightId = ""
    val utmZone = 32
    var flightId = ""
    var airplaneType = ""
    var originAirport = ""
    var destinationAirport = ""
    var tracks = ""
    var trackNumber = ""
    var trackStartDay: LocalDate?
    var trackStartTime: LocalTime?
    var startDateTime: LocalDateTime? = null
    var trackEndDay: LocalDate?
    var trackEndTime: LocalTime?
    var endDateTime: LocalDateTime? = null
    var flightTimestamps: MutableList<String> = mutableListOf("")
    fun extractMonthFromFileName(fileName: String): String? {
        val regex = Regex(".*_(\\d{2})\\d{2}\\.exp")
        return regex.find(fileName)?.groups?.get(1)?.value
    }
    var counter = 0
    if (directory.exists() && directory.isDirectory) {


        val expFiles = directory.listFiles { file -> file.extension == "exp" }
        var low = false
        val writer = CSVWriter(FileWriter(outputCsvFile))
        val header = "flightId,timestamp,airplaneType,originAirport,destinationAirport,track,latitude,longitude,altitude"
        writer.writeNext(header.split(",").toTypedArray())

        for (currentExpFile in expFiles){
            if(currentExpFile.name.contains("LOW"))low=true
            if(counter==totalNumberRows)break
            val fileMonth = extractMonthFromFileName(currentExpFile.name)?.toIntOrNull()
            if (fileMonth != null) {
                println("Processing file: ${currentExpFile.name}, extracted month: $fileMonth")
            } else {
                println("Processing file: ${currentExpFile.name}, month could not be extracted")
                continue
            }

            val reader = CSVReader(FileReader(currentExpFile))
            var row = 1

            try {
                // Remove entries where no longitude or latitude is given or entries for the same plane with the same timestamp
                var record: Array<String>? = reader.readNext()

                while (record != null) {

                    if (record.isEmpty() || record.size < 2){
                        record = reader.readNext()
                        row+=1
                    }

                    if (record != null) {

                        try {

                            if (record.size > 4) {

                                if (flightId!=record[0])flightTimestamps.clear()

                                flightId = record[0]
                                if(low) newFlightId = flightId + "0"
                                if(!low) newFlightId = flightId + "1"
                                airplaneType = record[1]
                                originAirport = record[2]
                                destinationAirport = record[3]
                                tracks = record[4]
                                trackNumber = record[5]
                                trackStartDay = LocalDate.parse(record[6], DateTimeFormatter.ISO_DATE)
                                trackStartTime = LocalTime.parse(record[7], DateTimeFormatter.ISO_TIME)
                                startDateTime = LocalDateTime.of(trackStartDay, trackStartTime)
                                trackEndDay = LocalDate.parse(record[8], DateTimeFormatter.ISO_DATE)
                                trackEndTime = LocalTime.parse(record[9], DateTimeFormatter.ISO_TIME)
                                endDateTime = LocalDateTime.of(trackEndDay, trackEndTime)

                                var numberTrackPoints = record[10]

                                metaData = !record.any { entry -> entry.isEmpty() }


                            } else {

                                var seconds = record[0]
                                var easting = record[1]
                                var northing = "5${record[2]}"
                                var altitude = record[3]
                                var degreePos = utmToDegree("$utmZone N $easting $northing")

                                val writeRecord = arrayOfNulls<String>(9)

                                writeRecord[0] = newFlightId
                                var actualTime = startDateTime?.plusSeconds(seconds.toDouble().toLong())?.format(formatter)
                                val actualMonth = actualTime?.split("-")?.get(1)?.toInt()

                                writeRecord[1] = actualTime
                                writeRecord[2] = airplaneType
                                writeRecord[3] = originAirport
                                writeRecord[4] = destinationAirport
                                writeRecord[5] = trackNumber
                                writeRecord[6] = degreePos.latitude.toString()
                                writeRecord[7] = degreePos.longitude.toString()
                                writeRecord[8] = altitude

                                if (metaData && record.none { entry -> entry.isEmpty() } && !flightTimestamps.contains(actualTime)) {
                                    if (actualMonth != null && actualMonth == fileMonth) {
                                        writer.writeNext(writeRecord)
                                        counter++
                                    }
                                }

                                if (actualTime != null) {
                                    flightTimestamps.add(actualTime)
                                }
                            }
                        }catch (e: Exception){
                            println("Could not process row: $row.")
                            metaData=false
                        }

                        record = reader.readNext()
                        row+=1
                        if(counter==totalNumberRows)break
                    }
                }
            } finally {
                println("Number of Rows in dataset: $counter.")
                reader.close()
            }
        }
        writer.close()

    } else {
        println("Directory does not exist or is not a directory")
    }
}


fun main(){

    // specify as parameter total number of rows the flightpoint data should contain. If all rows of the files should be included omit the parameter
    createFlightPointsMobilityDB()

    // creates trips for flights of all flight points
    // createFlightTripsGeomesa()
}
