import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.expressions.Window

import scala.collection.Searching.search

/* Calulates the MBR for a given set of valid longitude and latitude
 * values and outputs the three most frequent tiles for each device id.
 *
 * returns the MBR parameters, records processed and records filtered out
 *
 * Author: Michael Schroeder
 */

object SkyhookApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("SkyHook Application").getOrCreate()
    import spark.implicits._

// Static values
    val dataPath = "s3://skyhook-hr/qa/sg_sample2/*"
    val outputPath = "path/to/output"
    val dataFile = spark.read.format("csv").option("header", true).option("inferSchema", true).load(dataPath)

    // create 0-100 sequence to generate rectanble corner points
    val nums = Seq.range(0,100)
    val mbrFields = Seq("latitude", "longitude")
    val maxAggFields = mbrFields.map( c => max(c).cast(DoubleType).as(c))
    val minAggFields = mbrFields.map( c => min(c).cast(DoubleType).as(c))



// functions/methods
    // Returns a boolean value if lat/long have valid precision
    val invalidCoord = udf { (coord1: String, coord2: String) => {
      if(coord1.split('.')(1).length <= 4 || coord2.split('.')(1).length <= 4) true
      else false
      }
    }
    // Returns a boolean value if value is string null, empty or null
    val removeEmpty = udf { (s: String) => Option(s).map{
      case (value: String) => { value  == "null" || value == ""}
      }.getOrElse(true)
    }


    // filters out invalid values
    val filteredData = dataFile.filter(not($"HPE" > 50.0 || invalidCoord($"latitude", $"longitude") || removeEmpty($"device ID")))

    // returns a tuple of the max (lat/long)
    //val maxLatLong = filteredData.agg(maxAggFields.head, maxAggFields.tail: _*).as[(Double,Double)].head
    val maxLatLong = dataFile.agg(maxAggFields.head, maxAggFields.tail: _*).as[(Double,Double)].head

    // returns a tuple of the min (lat/long)
    //val minLatLong = filteredData.agg(minAggFields.head, minAggFields.tail: _*).as[(Double,Double)].head
    val minLatLong = dataFile.agg(minAggFields.head, minAggFields.tail: _*).as[(Double,Double)].head

    // creates uniform step values for lat/long to create rectance corners
    val latStep = (maxLatLong._1 - minLatLong._1) / 100
    val latGridVals = nums.map{ case (n: Int) => minLatLong._1 +  n * latStep}.toIndexedSeq

    val longStep = (maxLatLong._2 - minLatLong._2) / 100
    val longGridVals = nums.map{ case (n: Int) => minLatLong._2  +  n * longStep}.toIndexedSeq

    // runs a binary search which returns 1 above the tile value as it references the top right corner of the rectangle
    // takes into account the case where it its the min lat/long
    def findTile(latGrid: IndexedSeq[Double], longGrid: IndexedSeq[Double]) =  udf { ( lat: Double, long: Double) => {
      (latGrid.search(lat).insertionPoint, longGrid.search(long).insertionPoint) match {
        case pt if(pt._1 == 0 && pt._2 == 0) => (0,0).toString
        case pt if(pt._1 == 0) => (0, pt._2 - 1).toString
        case pt if(pt._2 == 0) => (pt._1 - 1, 0).toString
        case pt => (pt._1 - 1, pt._2 - 1 ).toString
        }
      }
    }

    // Functions and values that pertain to finding most visited tiles
    val rankWindow = Window.partitionBy($"device ID").orderBy($"recs".desc)
    val mostCommon = udf { (rank: Integer, tile: String) => if(rank == 1) tile else null }
    val secMostCommon = udf { (rank: Integer, tile: String) => if(rank == 2)  tile else null }
    val thdMostCommon = udf { (rank: Integer, tile: String) => if(rank == 3) tile else null }
    val firstCols = Seq("most_common", "sec_most_common", "thd_most_common")
    val firstAgg = firstCols.map( c => first(c, ignoreNulls=true).as(c))

// Pipes
    val tileData = dataFile
      .withColumn("tile", findTile(latGridVals, longGridVals)($"latitude", $"longitude"))
      .groupBy("device ID", "tile").agg(
        count(lit(1)).as("recs")
      )
      .withColumn("rank", rank.over(rankWindow))
      .filter($"rank" <= 3)
      .withColumn("most_common", mostCommon($"rank", $"tile"))
      .withColumn("sec_most_common", secMostCommon($"rank", $"tile"))
      .withColumn("thd_most_common", thdMostCommon($"rank", $"tile"))
      .groupBy("device ID").agg(firstAgg.head, firstAgg.tail: _*)
      .write.parquet(outputPath)


// printing summary metrics
    println(s"The MBR parameters are (${minLatLong._1}, ${minLatLong._2}), (${minLatLong._1}, ${maxLatLong._2}), (${maxLatLong._1}, ${minLatLong._2}),(${maxLatLong._1}, ${maxLatLong._2})")
    println(s"Total number of records processed is ====> ${dataFile.count}")
    println(s"Total number of invalid records processed is ====> ${dataFile.count - filteredData.count}")

    spark.stop()
  }
}
