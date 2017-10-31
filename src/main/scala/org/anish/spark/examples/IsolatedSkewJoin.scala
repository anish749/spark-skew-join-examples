package org.anish.spark.examples

import org.anish.spark.skew.Utils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import vegas._
import vegas.sparkExt._

import scala.io.StdIn


/**
  * Two data frames to be joined, one is skewed on one known value.
  * Both data frames are large which makes broadcasting not feasible.
  *
  * Created by anish on 31/10/17.
  */
object IsolatedSkewJoin {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[8]").appName("SkewJoin").getOrCreate()
    import sparkSession.implicits._
    import Utils.DataSetUtils

    // To run in local we dont want to use 200 partitions.
    sparkSession.conf.set("spark.sql.shuffle.partitions", "16")


    val prices_withFeatures = sparkSession
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("data/skewed_stock/prices_withFeatures")
      // This data is partitioned by symbol
      .cache

    println("Stats for Prices")
    prices_withFeatures.showPartitionStats()

    // Visualize prices data
    /*
    val prices_withFeatures_agg = prices_withFeatures
      .groupBy('symbol)
      .agg(count("*") as "symbolFreq")
    Vegas("prices for each symbol")
      .withDataFrame(prices_withFeatures_agg, 150)
      .encodeX("symbol", Nom)
      .encodeY("symbolFreq", Quant)
      .mark(Bar)
      .show
      */
    // This has skew on symbol 'AB'


    // Some other large data (Look Up) = Key is date, symbol
    val day_level_agg_lookup_data = sparkSession
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("data/skewed_stock/someCalculatedLargeData")

    println("Stats for day_level_agg_lookup_data")
    day_level_agg_lookup_data.showPartitionStats()


    // The following always gives 1, but we can see that there is no skew in this dataset
    //    // Visualize 2nd Data Frame
    /*val day_level_agg_histo = day_level_agg_lookup_data
      .withColumn("symbol_date", concat('symbol, lit("_"), 'date)) // because we can have only 1 col in x-axis
      .groupBy('symbol_date)
      .agg(count("*") as "cnt")
    Vegas("Symbol With Date Counts - 2nd DF")
      .withDataFrame(day_level_agg_histo, 150)
      .encodeX("symbol_date", Nom)
      .encodeY("cnt", Quant)
      .mark(Bar)
      .show*/

    val joinColumns = Array("symbol", "date")

    sparkSession.sparkContext.setJobGroup("1", "Simple Join of two large data sets, with skew")
    val joinedDf = prices_withFeatures.join(day_level_agg_lookup_data, joinColumns)


    joinedDf.timedSaveToDisk("joining large and large DF")
    println("Stats for Joined data")
    sparkSession.sparkContext.clearJobGroup()

    joinedDf.showPartitionStats()
    // The normal join doesn't remove skew


    // Isolated Fragmentation and Replication Join
    // Since prices is large, we fragment that first
    sparkSession.sparkContext.setJobGroup("2", "Isolated Fragmented replicated join")

    val replicationCount = 4
    val fragmented = prices_withFeatures.withColumn("salt", when('symbol === "AB", rand(1) * replicationCount cast IntegerType) otherwise 0)
    val replicated = day_level_agg_lookup_data.withColumn("replicaIndices", when('symbol === "AB", (0 until replicationCount) toArray) otherwise Array(0))
      .withColumn("salt", explode('replicaIndices))
      .drop('replicaIndices)

    val frJoined = fragmented.join(replicated, joinColumns :+ "salt")
    frJoined.timedSaveToDisk("joining large and large DF")
    println("Stats for Fragmented Replicated joined data")
    frJoined.showPartitionStats()
    sparkSession.sparkContext.clearJobGroup()

    println("Press return to Exit") // Keep Spark UI active (only for local machine)
    StdIn.readLine()
    sparkSession.stop()
  }

}

/*

Stats for Prices
Total Partitions -> 75
Total Records -> 3851264
Percentiles -> Min 	| 25th 	| 50th 	| 75th 	| Max
Percentiles -> 2004 	| 7504 	| 7761 	| 15280 	| 1875000
Stats for day_level_agg_lookup_data
Total Partitions -> 10
Total Records -> 515553
Percentiles -> Min 	| 25th 	| 50th 	| 75th 	| Max
Percentiles -> 7264 	| 56384 	| 56384 	| 56384 	| 58146
Elapsed time for joining large and large DF : 15890096205 ns or 15890.096205 ms
Stats for Joined data
Total Partitions -> 16
Total Records -> 3851264
Percentiles -> Min 	| 25th 	| 50th 	| 75th 	| Max
Percentiles -> 52851 	| 53132 	| 53199 	| 53618 	| 3052762
Elapsed time for joining large and large DF : 8866889890 ns or 8866.88989 ms   // Join happens faster
Stats for Fragmented Replicated joined data
Total Partitions -> 16
Total Records -> 3851264
Percentiles -> Min 	| 25th 	| 50th 	| 75th 	| Max
Percentiles -> 52609 	| 52906 	| 53501 	| 802267 	| 804002                    // Skew is reduced
Press return to Exit


 */