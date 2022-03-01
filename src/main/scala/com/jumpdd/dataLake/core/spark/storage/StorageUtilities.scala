package com.jumpdd.dataLake.core.spark.storage

import com.jumpdd.dataLake.core.Spark.currentSparkSession
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.col

object StorageUtilities {

  val hadoopConf = currentSparkSession.sparkContext.hadoopConfiguration

  def fullRemove(tableName: String, where: String): Unit = {
    val basePath = new Path(s"$where/$tableName")
    val fs = basePath.getFileSystem(hadoopConf)
    fs.delete(basePath, true)
  }

  def deltaRemove(dataFrame: DataFrame, tableName: String, where: String): Unit = {
    dataFrame.select(functions.min(col("daydate")))
  }
}
