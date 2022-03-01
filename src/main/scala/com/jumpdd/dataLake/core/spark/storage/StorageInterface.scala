package com.jumpdd.dataLake.core.spark.storage

import com.jumpdd.dataLake.core.spark.DateRange
import com.jumpdd.dataLake.core.spark.StorageEngine.defaultPartitions
import org.apache.spark.sql.DataFrame

trait StorageInterface {
  def read(tableName: String, where: String, dateRange: DateRange, partitions: Seq[String] = defaultPartitions): DataFrame
  def remove(dataFrame: DataFrame, tableName: String, where: String, isFull: Boolean, partitions: Seq[String] = defaultPartitions): Unit
  def write(dataFrame: DataFrame, tableName: String, where: String, partitions: Seq[String] = defaultPartitions): Unit
}
