package com.jumptvs.etls.processes

import com.jumptvs.etls.config.GlobalConfiguration
import com.jumptvs.etls.db.DateRange
import com.jumptvs.etls.reader.{LocalReader, S3Reader}
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.net.URI

trait BaseLoadProcess {

  val tableName: String
  private val uri: String => URI = path => new URI(path)

  def reader(dates: DateRange,
             isFull: Boolean,
             globalConfig: GlobalConfiguration,
             readerOptions: Map[String, String] = Map(),
             readerCondition: String = null,
             emptyDataFrameColumns: Seq[String] = null)(implicit sparkSession: SparkSession): DataFrame = {

    val protocol = uri(globalConfig.source.get.path).getScheme

    val preProcessedDF = protocol match {
      case "s3a" => S3Reader.readTable(dates, isFull, globalConfig, tableName, readerOptions, emptyDataFrameColumns).get
      case _ => LocalReader.readTable(dates, isFull, globalConfig, tableName, readerOptions, emptyDataFrameColumns).get
    }

    process(preProcessedDF, dates, globalConfig, readerCondition)
  }

  protected def process(df: DataFrame,
                        dates: DateRange,
                        globalConfig: GlobalConfiguration,
                        readerCondition: String = null): DataFrame
}
