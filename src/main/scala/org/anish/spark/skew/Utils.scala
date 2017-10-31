package org.anish.spark.skew

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql._
import org.scalatest.Matchers


/**
  * Few Utility functions for extending Spark Datasets for exploring partitions
  *
  * Created by anish on 22/10/17.
  */
object Utils extends Matchers {

  implicit class DataSetUtils[T](val dataset: Dataset[T]) {

    /**
      * Prints record counts per partition
      */
    def showCountPerPartition() = {
      println(countPerPartition.map(x => s"${x._1} => ${x._2}").mkString("Idx => Cnt\n", "\n", ""))
    }

    /**
      * Prints total partitions, records in an RDD
      * Counts values in each partitions and prints 4 important percentile counts
      */
    def showPartitionStats(extended: Boolean = false) = {
      val numPartitions = countPerPartition.length
      val sortedCounts = countPerPartition.map(_._2).sorted
      def percentileIndex(p: Int) = math.ceil((numPartitions - 1) * (p / 100.0)).toInt
      println(s"Total Partitions -> $numPartitions\n" +
        s"Total Records -> ${sortedCounts.map(_.toLong).sum}\n" + // One partition wont have records more than Int.MAX_VALUE
        s"Percentiles -> Min \t| 25th \t| 50th \t| 75th \t| Max\n" +
        s"Percentiles -> ${sortedCounts(percentileIndex(0))} \t| " +
        s"${sortedCounts(percentileIndex(25))} \t| " +
        s"${sortedCounts(percentileIndex(50))} \t| ${sortedCounts(percentileIndex(75))} \t| " +
        s"${sortedCounts(percentileIndex(100))}")
      if (extended) showCountPerPartition()
    }

    /**
      * Counts number of records per partition. Triggers an action
      *
      * @return List of tuple with partition index and count of records
      */
    lazy val countPerPartition: List[(Int, Int)] = { // Because the data set is immutable, we dont want to count multiple times
      dataset.rdd.mapPartitionsWithIndex { (index, iter) =>
        List((index, iter.size)).iterator
      }.collect.toList
    }

    // I know this is silly for Spark, better check the Spark UI and see the time required for each stage
    def timedSaveToDisk(operationName: String, tmpFilepath: String = s"data/tmp/${System.currentTimeMillis()}") = {
      time(operationName) {
        dataset.write.mode(SaveMode.Overwrite)
          .save(tmpFilepath)
      }

      FileUtils.deleteDirectory(new File(tmpFilepath))

      def time[R](blockName: String)(block: => R): R = {
        val t0 = System.nanoTime()
        val result = block // call-by-name
        val timeElapsedNano = System.nanoTime() - t0
        println(s"Elapsed time for $blockName : $timeElapsedNano ns or ${
          timeElapsedNano / 1e6
        } ms")
        result
      }
    }

    /**
      * Compares current dataset with other and ensures that they have the same schema (ignore nullable) and the same values
      * This collects both data frames in the driver, thus not suitable for very large test data. Good for unit testing.
      *
      * @param other  Some other dataset to compare with
      * @param onlySchema only compare the schemas of the datasets
      */
    def ensureDatasetEquals(other: Dataset[T], onlySchema: Boolean = false): Unit = {
      dataset.schema.map(f => (f.name, f.dataType)).toSet shouldBe other.schema.map(f => (f.name, f.dataType)).toSet
      if (!onlySchema) {
        dataset.collect.toSet shouldBe other.collect.toSet
      }
    }

  }

}