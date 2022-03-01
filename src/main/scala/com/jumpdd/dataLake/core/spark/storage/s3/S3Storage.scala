package com.jumpdd.dataLake.core.spark.storage.s3

import com.jumpdd.dataLake.core.Spark.currentSparkSession
import com.jumpdd.dataLake.core.spark.DateRange
import com.jumpdd.dataLake.core.spark.StorageEngine.defaultPartitions
import com.jumpdd.dataLake.core.spark.storage.StorageInterface
import org.apache.spark.sql.DataFrame

object S3Storage extends StorageInterface {
  override def read(tableName: String, where: String, dateRange: DateRange, partitions: Seq[String]): DataFrame = {
    null
  }

  override def remove(dataFrame: DataFrame, tableName: String, where: String, isFull: Boolean, partitions: Seq[String]): Unit = {
  }

  override def write(dataFrame: DataFrame, tableName: String, where: String, partitions: Seq[String]): Unit = {
  }
}
