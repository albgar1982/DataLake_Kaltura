package com.jumptvs.etls.reader


import com.jumptvs.etls.config.GlobalConfiguration
import com.jumptvs.etls.db.DateRange
import com.jumptvs.etls.model.Tmp
import com.jumptvs.etls.utils.DataUtils
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

object LocalReader extends Reader {

  override def readTable(dates: DateRange, isFull: Boolean, config: GlobalConfiguration, table: String, readerOptions: Map[String, String] = Map(), emptyDataFrameColumns: Seq[String] = null)(implicit sparkSession: SparkSession): Try[DataFrame] = {
    val (basePath, path) = loadReader(config, table)
    DataUtils.exists(path) match {
      case true => loadData(config.source.get.format, basePath, path, readerOptions)
      case false => createDataFrame(path, emptyDataFrameColumns)
    }
  }

  private def loadData(format: String, basePath: String, path: String, readerOptions: Map[String, String] = Map())(implicit sparkSession: SparkSession): Try[DataFrame] = {
    format match {
      case "parquet" => {
        Try(
          sparkSession.read
            .option("basePath", basePath)
            .format("parquet")
            .load(path)
            .withColumn(Tmp.JumpInputFileName, input_file_name)
        )
      }
      case "csv" => {
        Try(sparkSession.read
          .options(readerOptions)
          .option("header", "true")
          .format("csv")
          .load(path)
          .withColumn(Tmp.JumpInputFileName, input_file_name))
      }
    }
  }

  private def loadReader(config: GlobalConfiguration, table: String): (String, String) = {
    config.source.get.format match {
      case "csv" => (
        s"${config.source.get.path}/${table}",
        s"${config.source.get.path}/${table}/"
      )
      case _ =>
        (
          s"${config.source.get.path}/${table}/",
          s"${config.source.get.path}/${table}/brandid=${config.brandId.get}/"
        )
    }
  }

}
