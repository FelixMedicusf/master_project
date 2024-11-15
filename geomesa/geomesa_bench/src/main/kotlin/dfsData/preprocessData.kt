package geomesa.dfsData

import com.opencsv.CSVReader
import com.opencsv.CSVWriter
import java.io.File
import java.io.FileReader
import java.io.FileWriter
import java.time.*
import java.time.format.DateTimeFormatter

fun createFlightPointsGeomesa(totalNumberRows: Int = -1) {


    val inputDirectory = "../../data/dfsData"
    val outputCsvFile = "../../data/dfsData/output/FlightPointsGeomesa.csv"

    val directory = File(inputDirectory)
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
    var metaData = false

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

    var counter = 0
    if (directory.exists() && directory.isDirectory) {

        val expFiles = directory.listFiles { file -> file.extension == "exp" }

        val writer = CSVWriter(FileWriter(outputCsvFile))
        val header = "flightId,timestamp,airplaneType,originAirport,destinationAirport,track,latitude,longitude,altitude"
        writer.writeNext(header.split(",").toTypedArray())

        for (currentExpFile in expFiles){
            if(counter==totalNumberRows)break
            println("Processing file: ${currentExpFile.name}")
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

                                writeRecord[0] = flightId
                                var actualTime = startDateTime?.plusSeconds(seconds.toDouble().toLong())?.format(formatter)
                                writeRecord[1] = actualTime
                                writeRecord[2] = airplaneType
                                writeRecord[3] = originAirport
                                writeRecord[4] = destinationAirport
                                writeRecord[5] = "$trackNumber/$tracks"
                                writeRecord[6] = degreePos.latitude.toString()
                                writeRecord[7] = degreePos.longitude.toString()
                                writeRecord[8] = altitude

                                if (metaData && record.none { entry -> entry.isEmpty() } && !flightTimestamps.contains(actualTime)) {
                                    writer.writeNext(writeRecord)
                                    counter++
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

fun createFlightTripsGeomesa(splitTracks: Boolean = false) {
    val inputPath = "../../data/dfsData/output/FlightPointsGeomesa.csv"
    val outputCsvFile = "../../data/dfsData/output/FlightTripsGeomesa.csv"

    val inputFile = File(inputPath)

    val writeRecord = arrayOfNulls<String>(7)
    var sameFlight: Boolean
    var sameTrack = true
    var firstEntry = true


    if (inputFile.exists() && inputFile.isFile) {

        val writer = CSVWriter(FileWriter(outputCsvFile), ';', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END)
        val header = "flightId,airplaneType,originAirport,destinationAirport,timestamps,geoms,altitudes"
        writer.writeNext(header.split(",").toTypedArray())

        println("Processing file ${inputFile.name} to create Flight Trips.")

        val reader = CSVReader(FileReader(inputFile))

        var rows = 0
        val inputHeader = reader.readNext()

        try {

            var record: Array<String>? = reader.readNext()

            while (record != null) {

                try {

                    var flightId = record[0]
                    var timestamp = record[1]
                    var airplaneType = record[2]
                    var origin = record[3]
                    var destination = record[4]
                    var track = record[5]
                    var latitude = record[6]
                    var longitude = record[7]
                    var altitude = record[8]

                    if (firstEntry) {

                        writeRecord[0] = flightId
                        writeRecord[1] = airplaneType
                        writeRecord[2] = origin
                        writeRecord[3] = destination
                        writeRecord[4] = timestamp
                        writeRecord[5] = "MULTILINESTRING(($longitude $latitude"
                        writeRecord[6] = altitude
                        firstEntry=false

                    }else{
                        writeRecord[4] = writeRecord[4] + ",$timestamp"

                        if (splitTracks) {
                            if (sameTrack) writeRecord[5] = writeRecord[5] + ", $longitude $latitude" else writeRecord[5] = writeRecord[5] + "), ($longitude $latitude"
                        }
                        else writeRecord[5] = writeRecord[5] + ", $longitude $latitude"


                        writeRecord[6] = writeRecord[6] + ",$altitude"

                    }

                    record = reader.readNext()

                    //if (row==300)break
                    if (record == null)break

                    var nextFlightId = record[0]
                    var nextTrack = record[5]


                    sameFlight=flightId==nextFlightId
                    sameTrack=track==nextTrack


                    if(!sameFlight){
                        writeRecord[5] = writeRecord[5] + "))"
                        rows ++
                        writer.writeNext(writeRecord)
                        firstEntry=true
                    } else {
                        firstEntry=false
                    }

                } catch (e: Exception) {
                    e.printStackTrace()
                }
            }

        } finally {
            println("Number of Rows in trip dataset: $rows.")
            reader.close()
        }

        writer.close()

    } else {
        println("File does not exist or is not a directory")
    }

}

fun main(){

    // specify as parameter total number of rows the flightpoint data should contain. If all rows of the files should be included omit the parameter
    createFlightPointsGeomesa(10_000_000)

    // creates trips for flights of all flight points
    createFlightTripsGeomesa(true)
}
