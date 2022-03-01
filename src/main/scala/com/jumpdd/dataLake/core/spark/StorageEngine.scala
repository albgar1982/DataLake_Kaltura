package com.jumpdd.dataLake.core.spark

import com.jumpdd.dataLake.core.Spark.currentSparkSession
import com.jumpdd.dataLake.core.spark.storage.StorageUtilities
import com.jumpdd.dataLake.core.spark.storage.hdfs.HDFSStorage
import com.jumpdd.dataLake.core.spark.storage.s3.S3Storage
import com.jumpdd.dataLake.core.utilities.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, functions}

import java.net.URI
import java.time.LocalDate

case class DateRange(start: LocalDate, end: LocalDate)

object StorageEngine extends Logger {

  private object StorageTypes {
    val S3 = "s3a"
    val HDFS = "hdfs"
    val LOCAL = "./"
  }

  val defaultPartitions = Seq("brandid", "daydate")

  private val uri: String => URI = path => new URI(path)

  def remove(dataFrame: DataFrame, tableName: String, where: String, isFull: Boolean, partitions: Seq[String] = defaultPartitions): Unit = {
    currentSparkSession.sparkContext.setJobGroup(s"Storage", s"Removing data of table '$tableName'")
    info(s"tableName ~> '$tableName' where ~> '$where' isFull '$isFull'")

    if (isFull) {
      StorageUtilities.fullRemove(tableName.toLowerCase, where)
      return
    }

    StorageUtilities.deltaRemove(dataFrame, tableName.toLowerCase, where)

    /*uri(where).getScheme match {
      case StorageTypes.S3 => S3Storage.remove(dataFrame, tableName, where, isFull, partitions)
      case StorageTypes.HDFS => HDFSStorage.remove(dataFrame, tableName, where, isFull, partitions)
    }*/
  }

  def read(tableName: String, where: String, dateRange: DateRange, partitions: Seq[String] = defaultPartitions): DataFrame = {
    currentSparkSession.sparkContext.setJobGroup(s"Storage", s"Reading data of table '$tableName'")
    info(s"tableName ~> '$tableName' where ~> '$where'")

    uri(where).getScheme match {
      case StorageTypes.S3 => S3Storage.read(tableName.toLowerCase(), where, dateRange, partitions)
      case StorageTypes.HDFS | _ => HDFSStorage.read(tableName.toLowerCase, where, dateRange, partitions)
    }
  }

  def write(dataFrame: DataFrame, tableName: String, where: String, partitions: Seq[String] = defaultPartitions): Unit = {

    info(s"tableName ~> '$tableName' where ~> '$where'")

    currentSparkSession.sparkContext.setJobGroup(s"Storage", s"Writing data of table '$tableName'")

    val columnsToRepartition = Seq(partitions).filterNot(_ == "brandid")
    val path = s"$where/${tableName.toLowerCase}"

    dataFrame
      //.repartition(columnsToRepartition: _*) // TODO: Dynamic build columns
      .write
      .mode(SaveMode.Append)
      .partitionBy(partitions: _*)
      .parquet(path)

    /*
    uri(where).getScheme match {
      case StorageTypes.S3 => S3Storage.write(dataFrame, tableName, where, partitions)
      case StorageTypes.HDFS => HDFSStorage.write(dataFrame, tableName, where, partitions)
    }

    */
  }
}