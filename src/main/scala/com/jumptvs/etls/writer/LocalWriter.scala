package com.jumptvs.etls.writer

import com.jumptvs.etls.config.GlobalConfiguration
import com.jumptvs.etls.model.Global
import com.jumptvs.etls.remover.Remover
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode}
object LocalWriter extends Writer {

  override def write(isFull: Boolean, config: GlobalConfiguration, df: DataFrame, table: String, columns: Seq[String]= null): Unit = {


    Remover.remove(df.sparkSession, s"./results/${table}/brandid=${config.brandId.get}")

    df
      .na
      .fill(Global.naColName)
      .repartition(col(Global.daydateColName))
      .write
      .partitionBy(Seq(Global.brandidColName, Global.daydateColName): _*)
      .format(config.target.get.format)
      .mode(SaveMode.Append)
      .option("header", "true")
      .save(s"./results/${table}")
  }
}
