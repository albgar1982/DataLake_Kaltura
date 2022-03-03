package dataLake.core.spark.storage

import dataLake.core.spark.StorageEngine.defaultPartitions
import dataLake.core.spark.DateRange
import org.apache.spark.sql.DataFrame

trait StorageInterface {
  def read(tableName: String, where: String, dateRange: DateRange, partitions: Seq[String] = defaultPartitions, schema: String): DataFrame
  def remove(dataFrame: DataFrame, tableName: String, where: String, isFull: Boolean, partitions: Seq[String] = defaultPartitions): Unit
  def write(dataFrame: DataFrame, tableName: String, where: String, partitions: Seq[String] = defaultPartitions): Unit
}
