package dataLake.core.layers.data_warehouse.registereduser.transformations

import dataLake.core.KnowledgeDataComputing
import dataLake.core.layers.data_warehouse.registereduser.registereduserModel.{activationdate, deactivationdate, eventdate, valuetype}
import dataLake.core.layers.{PlaybacksColumns, UsersColumns}
import dataLake.core.spark.DataFrameFactory
import dataLake.core.utilities.Logger
import org.apache.spark.sql.{DataFrame, Dataset, Row, functions}
import org.apache.spark.sql.functions.{col, lit, not, when}

object SnapshotTransformation extends Logger {

  def process(originDF: DataFrame): DataFrame = {
    val dragDF = dragStateDuration(originDF)
    val normalizeDF = normalize(dragDF)
    normalizeDF.withColumn(UsersColumns.brandid, lit(KnowledgeDataComputing.brandId))
  }

  def dragStateDuration(originDF: DataFrame): DataFrame = {
    val datesDF = DataFrameFactory
      .betweenDates(
        KnowledgeDataComputing.executionControl.start,
        KnowledgeDataComputing.executionControl.end
      )

    originDF.join(
      datesDF, datesDF.col(UsersColumns.daydate).between(originDF.col(UsersColumns.activationdaydate), originDF.col(UsersColumns.deactivationdaydate)), "inner"
    )
  }

  def normalize(originDF: DataFrame): DataFrame = {
    originDF
      .withColumn(
        UsersColumns.cancellationtype, when(col(UsersColumns.deactivationdaydate).notEqual(UsersColumns.daydate), lit("NA"))
          .otherwise(col(UsersColumns.cancellationtype))
      ).withColumn(
      UsersColumns.deactivationdaydate, when(col(UsersColumns.deactivationdaydate).notEqual(UsersColumns.daydate), lit("NA"))
        .otherwise(col(UsersColumns.deactivationdaydate))
    )
  }
}
