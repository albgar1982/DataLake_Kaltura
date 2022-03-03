package dataLake.core.spark.storage.hdfs

import dataLake.core.KnowledgeDataComputing.sparkSession
import dataLake.core.spark.DateRange
import dataLake.core.spark.storage.StorageInterface
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.types.{DataType, StructType}

object HDFSStorage extends StorageInterface {

  override def read(tableName: String, where: String, dateRange: DateRange, partitions: Seq[String], schema: String = null): DataFrame = {
    var originDF: DataFrame = null


    try {
      if (schema != null) {
        val dataSchema = DataType.fromJson(schema).asInstanceOf[StructType]

        originDF = sparkSession
          .read
          .schema(dataSchema)
          .parquet(s"$where/$tableName")
          .where(functions.col("daydate").geq("20220101"))
      } else {
        originDF = sparkSession
          .read
          .parquet(s"$where/$tableName")
          .where(functions.col("daydate").geq("20220101"))
      }
    } catch {
      case t: Throwable =>
        println(t.getLocalizedMessage)
    }

    originDF
  }

  def remove(dataFrame: DataFrame, tableName: String, where: String, isFull: Boolean, partitions: Seq[String]): Unit = {}

  def write(dataFrame: DataFrame, tableName: String, where: String, partitions: Seq[String]): Unit = {}
}
