package dfsData

import com.opencsv.CSVParserBuilder
import com.opencsv.CSVReader
import com.opencsv.CSVReaderBuilder
import com.opencsv.CSVWriter
import java.io.FileReader
import java.io.FileWriter

import org.locationtech.proj4j.CRSFactory
import org.locationtech.proj4j.CoordinateTransformFactory
import org.locationtech.proj4j.ProjCoordinate

fun preprocessRegionalData(inputFile: String, outputFile: String){
    val inputFile = "../../data/nrwData/$inputFile"
    val outputFile = "../../data/nrwData/output/$outputFile"


    val reader = CSVReaderBuilder(FileReader(inputFile)).withCSVParser(CSVParserBuilder().withSeparator(';').build()).build()


    println("Processing File $inputFile")
    val writer = CSVWriter(
        FileWriter(outputFile),
        ';',// Use default separator (`,`)
        CSVWriter.NO_QUOTE_CHARACTER, // Disable quotes
        CSVWriter.DEFAULT_ESCAPE_CHARACTER,
        CSVWriter.DEFAULT_LINE_END
    )

    val header = "name;polygon"
    writer.writeNext(header.split(";").toTypedArray())


    try {
        // Remove entries where no longitude or latitude is given or entries for the same plane with the same timestamp
        var record: Array<String>? = reader.readNext()


        val crsFactory = CRSFactory()
        val srcCrs = crsFactory.createFromName("EPSG:25832") // Source CRS (ETRS89 / UTM 32N)
        val dstCrs = crsFactory.createFromName("EPSG:4326")
        val transformFactory = CoordinateTransformFactory()
        val transform = transformFactory.createTransform(srcCrs, dstCrs)
        var region = ""
        var firstLocation = ""
        var counter = 0
        var firstEntry = true
        var ignoreEntries = false
        var polygon = ""
        var beginning = true
        while (record != null) {

                if(record.size > 3){
                    region = record[4].replace("ö", "oe").replace("ß", "ss").replace("ä", "ae").replace("ü", "ue")
                    firstEntry = true
                    counter = 0
                    if (!beginning) {
                        var outputLine = "$region;$polygon))"
                        writer.writeNext(outputLine.split(";").toTypedArray())
                    }
                    beginning=false

                    ignoreEntries=false
                } else {
                    if (firstEntry){
                        val sourceCoordinates = ProjCoordinate(record[1].toDouble(), record[2].toDouble())
                        val targetCoordinates = ProjCoordinate()
                        var transformedCoordinates = transform.transform(sourceCoordinates, targetCoordinates)
                        firstLocation=record[1]+record[2]
                        polygon = "POLYGON((${transformedCoordinates.x} ${transformedCoordinates.y}"
                        firstEntry=false
                    }
                    if (record[1]+record[2] == firstLocation)counter++



                    if(!ignoreEntries) {

                        val sourceCoordinates = ProjCoordinate(record[1].toDouble(), record[2].toDouble())
                        val targetCoordinates = ProjCoordinate()
                        var transformedCoordinates = transform.transform(sourceCoordinates, targetCoordinates)
                        polygon = "${polygon}, ${transformedCoordinates.x} ${transformedCoordinates.y}"
                        //var outputLine = "$region,${transformedCoordinates.x},${transformedCoordinates.y}"

                    }
                    if (counter==2)ignoreEntries=true
                }

                record = reader.readNext()

        }

    }catch (e: Exception){
        e.printStackTrace()
    }finally {
        reader.close()
        writer.close()
    }


}

fun main(){
    preprocessRegionalData("dvg1bld_nw.txt", "geomesa-state.csv")
    preprocessRegionalData("dvg1gem_nw.txt", "geomesa-municipalities.csv")
    preprocessRegionalData("dvg1krs_nw.txt", "geomesa-counties.csv")
    preprocessRegionalData("dvg1rbz_nw.txt", "geomesa-districts.csv")
}