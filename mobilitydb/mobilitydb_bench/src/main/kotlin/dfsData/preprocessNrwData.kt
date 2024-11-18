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
    val writer = CSVWriter(FileWriter(outputFile))

    val header = "name,latitude,longitude"
    writer.writeNext(header.split(",").toTypedArray())


    try {
        // Remove entries where no longitude or latitude is given or entries for the same plane with the same timestamp
        var record: Array<String>? = reader.readNext()


        val crsFactory = CRSFactory()
        val srcCrs = crsFactory.createFromName("EPSG:25832") // Source CRS (ETRS89 / UTM 32N)
        val dstCrs = crsFactory.createFromName("EPSG:4326")
        val transformFactory = CoordinateTransformFactory()
        val transform = transformFactory.createTransform(srcCrs, dstCrs)
        var region = ""
        while (record != null) {

                if(record.size > 3){
                    region = record[4]

                }else {
                    val sourceCoordinates = ProjCoordinate(record[1].toDouble(), record[2].toDouble())
                    val targetCoordinates = ProjCoordinate()
                    var transformedCoordinates = transform.transform(sourceCoordinates, targetCoordinates)
                    var outputLine = "$region,${transformedCoordinates.x},${transformedCoordinates.y}"

                    writer.writeNext(outputLine.split(",").toTypedArray())
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
    preprocessRegionalData("dvg1bld_nw.txt", "Bundesland.csv")
    preprocessRegionalData("dvg1gem_nw.txt", "Gemeinden.csv")
    preprocessRegionalData("dvg1krs_nw.txt", "Kreise.csv")
    preprocessRegionalData("dvg1rbz_nw.txt", "Regierungsbezirke.csv")
}