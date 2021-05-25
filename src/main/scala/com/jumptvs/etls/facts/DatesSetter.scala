package com.jumptvs.etls.facts

import com.jumptvs.etls.config.GlobalConfiguration
import com.jumptvs.etls.transformations.{TransformApplyCorrectFormatDateUDF, TransformDatesUDF}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

case class InputData(df: DataFrame, column: Column)

object DatesSetter extends BaseLoadExecution[InputData, DataFrame] {

  private val event = "timestamp"
  private val eventJump = "timestampEventJump"

  def process(inputData: InputData, config: GlobalConfiguration): DataFrame = {
    val computeDates = inputData.df.withColumn(eventJump,
      TransformDatesUDF.transformationUDF(inputData.column))
    cleanAndExplode(computeDates, event)
  }

  def dateFixer(df: DataFrame, column: String, format: String): DataFrame = {
    df.withColumn(column, TransformApplyCorrectFormatDateUDF.transformationUDF(format)(col(column)))
  }

}
