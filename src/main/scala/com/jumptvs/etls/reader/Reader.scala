package com.jumptvs.etls.reader

import java.nio.charset.StandardCharsets

import com.jumptvs.etls.config.GlobalConfiguration
import com.jumptvs.etls.db.DateRange
import com.jumptvs.etls.model.Global
import com.jumptvs.etls.utils.DataFrameUtils
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try


trait Reader {

  private val readByte = udf {
    values: Array[Byte] =>
      values match {
        case null => Global.naColName
        case _ => new String(values, StandardCharsets.UTF_8).replaceAll("^\"|\"$", "")
      }
  }

  def readBinary(df: DataFrame): DataFrame =  {
    df.schema.seq.foldLeft(df) {
      case (df, value) if value.dataType == BinaryType => df.withColumn(value.name, readByte(col(value.name)))
      case (df, value) if value.dataType != BinaryType => df.withColumn(value.name, col(value.name))
    }
  }

  def createDataFrame(path: String, emptyDataFrameColumns: Seq[String])(implicit sparkSession: SparkSession): Try[DataFrame] = {
    Option(emptyDataFrameColumns) match {
      case Some(columns) => Try(DataFrameUtils.createEmptyDataFrame(columns))
      case _ => throw new Exception("Path does not exist : " + path)
    }
  }

  def readTable(
                 dates: DateRange,
                 isFull: Boolean,
                 userActivityConfig: GlobalConfiguration,
                 table: String,
                 readerOptions: Map[String, String] = Map(),
                 emptyDataframeColumns: Seq[String] = null
               )(implicit sparkSession: SparkSession): Try[DataFrame]
}
