package org.anish.spark.examples

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Created by anish on 19/10/17.
  */
object CreateRandomSkewedData {
  val sparkSession = SparkSession.builder().master("local[8]").appName("SkewJoinDataCreator").getOrCreate()

  import sparkSession.implicits._

  /**
    * Create a small data set. For lookups and broadcast
    */
  def createSomeSmallDataSet(): Unit = {
    val saveTo: String = "data/skewed_stock/fundamentals/"
    val fundamentals_raw = sparkSession
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("data/nyse/fundamentals.csv")

    val fundamentals_df = fundamentals_raw.toDF(fundamentals_raw.columns.map(x => x.trim.toLowerCase): _*)
      .withColumn("symbol", col("ticker symbol").substr(0, 2))
      .groupBy('symbol)
      .agg(sum("accounts payable") as 'accounts_payable, sum("accounts receivable") as 'accounts_receivable)

    fundamentals_df
      .repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(saveTo)

    println(s"Created small DataSet at $saveTo with ${fundamentals_df.count} records")
  }

  /**
    * Returns a DF with around 800K records. Doesn't have a huge skew
    *
    * @return DataFrame
    */
  private lazy val someLargeDataFrame: DataFrame = {
    val prices_raw = sparkSession
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("data/nyse/prices.csv")
      .withColumn("symbol", 'symbol.substr(0, 2))
      .cache

    /*
    val symbolFreq = prices_raw
      .groupBy('symbol)
      .agg(count("*") as "freq")
    val plot = Vegas("Symbol Frequency")
      .withDataFrame(symbolFreq, 150)
      .encodeX("symbol", Nom)
      .encodeY("freq", Quant)
      .mark(Bar)
      .show
      */

    prices_raw
  }

  /**
    * Create a large data frame with 800K records, but not partitioned by any column
    */
  def createSomeLargeDataset(): Unit = {
    val prices_raw = someLargeDataFrame
    prices_raw
      .repartition(4)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save("data/skewed_stock/prices_raw/")

    println(s"Created a large data set with ${prices_raw.count} records")
  }

  /**
    * Create a large dataset of 3M records with a heavy skew for one particular column value
    */
  def createSkewedLargeDataSet(): Unit = {
    val prices_raw = someLargeDataFrame

    val real_skew = //|2010-01-25|    AB| 26.790001|54.699952|     26.68| 55.50995|1.53944E7|
      (1 to 1e6.toInt).map(x => ("2010-01-25", "AB", 1.2, 9.8, 4.5, 78.9, 1235543))
    val real_skew_df = sparkSession.sparkContext.parallelize(real_skew).toDF("date", "symbol", "f1", "f2", "f3", "f4", "f5")

    prices_raw
      .withColumnRenamed("open", "f1")
      .withColumnRenamed("close", "f2")
      .withColumnRenamed("low", "f3")
      .withColumnRenamed("high", "f4")
      .withColumnRenamed("volume", "f5")
      .union(real_skew_df) // Add skew here
      .union(real_skew_df)
      .union(real_skew_df)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .partitionBy("symbol")
      .mode(SaveMode.Overwrite)
      .save("data/skewed_stock/prices_withFeatures/")

    println(s"Created a large skewed data set")
  }

  /**
    * Returns an aggregated DataFrame which can be joined based on symbol and date with the other large data set. Has around 510K records
    *
    * @return
    */
  private lazy val anotherLargeDataSet: DataFrame = {
    someLargeDataFrame
      .groupBy('date, 'symbol)
      .agg(min('open) as 'open, max('close) as 'close, min('low) as 'low, max('high) as 'high, sum('volume) as 'volume)
      .cache()
  }

  /**
    * Creates a large dataset partitioned by symbol. This is aggregated data with around 510K records. Doesn't have duplicates on symbol and date column.
    * This is not heavily skewed.
    */
  def createAnotherLargeDataSetWithNoSkew(): Unit = {
    anotherLargeDataSet // This was grouped by 'symbol and 'date, and hence would produce 200 partitions, which if partitioned by 'symbol, would produce 200* 74 files max
      .repartition('symbol) // I want only one file per partition in the output, so that we dont encounter the small file problem
      .write
      .partitionBy("symbol")
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save("data/skewed_stock/someCalculatedLargeData/") // This is grouped by  .groupBy('date, 'symbol)

    println(s"Created aggregated large skewed data set with no skew")
  }

  /**
    * Creates a large dataset partitioned by symbol. This is aggregated data with around 2.5 records.
    * This has one duplicate for symbol and date and this is heavily skewed a single value different from our skewed prices dataset.
    */
  def createAnotherLargeDataSetWithDifferentSkew(): Unit = {
    val real_agg_skew =
      (1 to 1e6.toInt).map(x => ("2010-01-25", "AA", 1.2, 9.8, 4.5, 78.9, 1235543))
    val real_agg_skew_df = sparkSession.sparkContext.parallelize(real_agg_skew).toDF("date", "symbol", "open", "close", "low", "high", "volume")
    // Day level agg with one duplicate
    anotherLargeDataSet // This was grouped by 'symbol and 'date, and hence would produce 200 partitions, which if partitioned by 'symbol, would produce 200* 74 files max
      .union(real_agg_skew_df)
      .union(real_agg_skew_df)
      .repartition('symbol) // I want only one file per partition in the output, so that we dont encounter the small file problem
      .write
      .partitionBy("symbol")
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save("data/skewed_stock/someCalculatedLargeData_withdups/") // This is grouped by  .groupBy('date, 'symbol)

    println(s"Created aggregated large skewed data set with skew (would have dups)")
  }

  def main(args: Array[String]): Unit = {
    createSomeSmallDataSet()
    createSomeLargeDataset()
    createSkewedLargeDataSet()
    createAnotherLargeDataSetWithNoSkew()
    createAnotherLargeDataSetWithDifferentSkew()
  }

}
