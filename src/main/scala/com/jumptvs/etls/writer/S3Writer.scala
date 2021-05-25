package com.jumptvs.etls.writer

import java.net.URI

import com.jumptvs.etls.config.GlobalConfiguration
import com.jumptvs.etls.model.Global
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SaveMode}

object S3Writer extends Writer with LazyLogging {

  private val uri: String => URI = path => new URI(path)

  private def cleanDataFrame(df: DataFrame, columns: Seq[String]): DataFrame = {
    df
      .na
      .fill("NA", columns)
  }

  private def writer(isFull: Boolean, config: GlobalConfiguration, df: DataFrame, table: String = ""): Unit = {
    val savePath = s"${config.target.get.path}/${table}"

    df
      .repartition(df.col(Global.daydateColName))
      .write
      .partitionBy(Seq(Global.brandidColName, Global.daydateColName): _*)
      .format(config.target.get.format)
      .mode(SaveMode.Append)
      .save(savePath)
  }

  override def write(isFull: Boolean, config: GlobalConfiguration, df: DataFrame, table: String = "", columns: Seq[String] = null): Unit = {
    if (columns != null) writer(isFull, config, cleanDataFrame(df, columns), table)
    else writer(isFull, config, df, table)
  }
}
