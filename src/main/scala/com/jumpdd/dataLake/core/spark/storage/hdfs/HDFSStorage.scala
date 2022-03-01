package com.jumpdd.dataLake.core.spark.storage.hdfs

import com.jumpdd.dataLake.core.Spark.currentSparkSession
import com.jumpdd.dataLake.core.spark.DateRange
import com.jumpdd.dataLake.core.spark.storage.{StorageInterface, StorageUtilities}
import org.apache.spark.sql.DataFrame

object HDFSStorage extends StorageInterface {

  override def read(tableName: String, where: String, dateRange: DateRange, partitions: Seq[String]): DataFrame = {
    var originDF: DataFrame = null
    try {
      originDF = currentSparkSession
        .read
        .parquet(s"$where/$tableName")
    } catch {
      case _: Throwable => println("cosas")
    }

    originDF
  }

  def remove(dataFrame: DataFrame, tableName: String, where: String, isFull: Boolean, partitions: Seq[String]): Unit = {}

  def write(dataFrame: DataFrame, tableName: String, where: String, partitions: Seq[String]): Unit = {}
}
