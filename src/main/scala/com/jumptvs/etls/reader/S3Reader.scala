package com.jumptvs.etls.reader

import java.net.URI

import com.jumptvs.etls.config.GlobalConfiguration
import com.jumptvs.etls.db.DateRange
import com.jumptvs.etls.model.Tmp
import com.jumptvs.etls.utils.DataUtils
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

object S3Reader extends Reader with LazyLogging {

  private val uri: String => URI = path => new URI(path)

  override def readTable(dates: DateRange, isFull: Boolean, config: GlobalConfiguration, table: String, readerOptions: Map[String, String] = Map(), emptyDataFrameColumns: Seq[String] = null)(implicit sparkSession: SparkSession): Try[DataFrame] = {
    val basePath = loadReader(config, table, isFull)

    println(s"[S3Reader][${config.environment.get}] S3 Reader Base Path ${basePath}")

    DataUtils.exists(basePath) match {
      case true => loadData(config, isFull, basePath, dates)
      case false => createDataFrame(basePath, emptyDataFrameColumns)
    }
  }

  private def loadData(config: GlobalConfiguration, isFull: Boolean, basePath:String, dates: DateRange)(implicit sparkSession: SparkSession): Try[DataFrame] = {
    val paths = ReaderUtilities.buildBrandAndDaydatePaths(
      config: GlobalConfiguration,
      sparkSession,
      basePath,
      isFull,
      dates.start,
      dates.end
    )

    config.source.get.format match {
      case "parquet" => loadParquet(basePath, paths)
      case "csv" => loadCSV(basePath, paths)
    }
  }

  private def loadReader(config: GlobalConfiguration, table: String, isFull: Boolean): String = {

    s"${config.source.get.path}${table}/"
  }

  private def loadParquet(basePath: String, paths: Array[String])(implicit sparkSession: SparkSession): Try[DataFrame] = {

    Try(
      sparkSession.read.option("basePath", basePath)
        .parquet(paths: _*)
        .withColumn(Tmp.JumpInputFileName, input_file_name)
    )
  }

  private def loadCSV(basePath: String, paths: Array[String])(implicit sparkSession: SparkSession): Try[DataFrame] = {
    Try(readBinary(sparkSession.read.option("basePath", basePath)
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(paths: _*)
      .withColumn(Tmp.JumpInputFileName, input_file_name)))
  }

  private def getBasePath(config: GlobalConfiguration): String = {
    uri(config.source.get.path).getScheme + "://" + uri(config.source.get.path).getHost
  }

}
