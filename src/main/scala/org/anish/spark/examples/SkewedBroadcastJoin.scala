package org.anish.spark.examples

import org.anish.spark.skew.Utils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import scala.io.StdIn

/**
  * One Dataset has skew (large), and another is small enough to be broad casted.
  * We know the value on which the large data set is skewed.
  *
  * Created by anish on 31/10/17.
  */
object SkewedBroadcastJoin {

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

    val fundamentals = sparkSession // This is some small look up data
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("data/skewed_stock/fundamentals/")
      // This data is partitioned by symbol
      .cache

    println("Stats for Fundamentals")
    fundamentals.showPartitionStats()

    // Simple broadcast Join
    sparkSession.sparkContext.setJobGroup("1", "Simple Broadcast Join using DataSet")
    // Broadcast join DF
    val broadcastJoined = prices_withFeatures.join(broadcast(fundamentals.as("fundamentals")), "symbol")

    broadcastJoined.timedSaveToDisk("Simple broadcast join")
    broadcastJoined.showPartitionStats() // This still has skew, so further aggregations / joins if any would not be running in parallel
    sparkSession.sparkContext.clearJobGroup()


    sparkSession.sparkContext.setJobGroup("1", "Fragmented Broadcast Join using DataSet")
    val fragmentationCount = 8
    val fragmented = prices_withFeatures.withColumn("salt", when('symbol === "AB", rand(1) * fragmentationCount cast IntegerType) otherwise 0)
      .repartition('symbol, 'salt) // Re partition to remove skew - Remember we have 16 partitions total set by spark.sql.shuffle.partitions

    val fragmentedBroadcastJoined = fragmented.join(broadcast(fundamentals), "symbol")
    fragmentedBroadcastJoined.timedSaveToDisk("Fragmented Broadcast join")

    println("Partition stats after fragmentation")
    fragmented.showPartitionStats()

    println("Partition stats for joined broadcast joined df")
    fragmentedBroadcastJoined.showPartitionStats()
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
Stats for Fundamentals
Total Partitions -> 1
Total Records -> 275
Percentiles -> Min 	| 25th 	| 50th 	| 75th 	| Max
Percentiles -> 275 	| 275 	| 275 	| 275 	| 275
Elapsed time for Simple broadcast join : 6443885346 ns or 6443.885346 ms
Total Partitions -> 75
Total Records -> 3807745
Percentiles -> Min 	| 25th 	| 50th 	| 75th 	| Max
Percentiles -> 1236 	| 6694 	| 7082 	| 15180 	| 1875000                 // Output data still has skew
Elapsed time for Fragmented Broadcast join : 5224417415 ns or 5224.417415 ms
Partition stats after fragmentation
Total Partitions -> 16
Total Records -> 3851264
Percentiles -> Min 	| 25th 	| 50th 	| 75th 	| Max
Percentiles -> 31992 	| 47554 	| 72271 	| 429386 	| 824921              // Skew is reduced
Partition stats for joined broadcast joined df
Total Partitions -> 16
Total Records -> 3807745
Percentiles -> Min 	| 25th 	| 50th 	| 75th 	| Max
Percentiles -> 28468 	| 44050 	| 70509 	| 425862 	| 821019
Press return to Exit


 */