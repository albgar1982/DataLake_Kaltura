package dataLake.core.spark.storage.s3

import dataLake.core.spark.DateRange
import dataLake.core.spark.storage.StorageInterface
import org.apache.spark.sql.DataFrame

object S3Storage extends StorageInterface {
  override def read(tableName: String, where: String, dateRange: DateRange, partitions: Seq[String], schema: String = null): DataFrame = {
    null
  }

  override def remove(dataFrame: DataFrame, tableName: String, where: String, isFull: Boolean, partitions: Seq[String]): Unit = {
  }

  override def write(dataFrame: DataFrame, tableName: String, where: String, partitions: Seq[String]): Unit = {
  }
}
