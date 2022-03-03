package dataLake.core.layers.data_warehouse.playbackseriesactivity.transformations

import dataLake.core.KnowledgeDataComputing
import dataLake.core.layers.PlaybacksColumns
import dataLake.core.utilities.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, not}

object FilterContentsSeriesTransformation extends Logger {

  def process(originDF: DataFrame): DataFrame = {
    val filteredByContentTypesDF = filterByContentTypes(originDF)
    filteredByContentTypesDF.show()
    excludeByConsumptionTypes(filteredByContentTypesDF)
  }

  def filterByContentTypes(originDF: DataFrame): DataFrame = {
    val filterByContentTypes = KnowledgeDataComputing.mapping.obj("filterByContentTypes").arr.mkString(",").replaceAll("\\\"", "").split(",")

    originDF
      .where(col(PlaybacksColumns.contenttype)
        .isin(filterByContentTypes:_*)
      )
  }

  def excludeByConsumptionTypes(originDF: DataFrame): DataFrame = {

    KnowledgeDataComputing.mapping.obj.contains("excludeByConsumptionTypes") match {
      case false => originDF
      case true =>
        val excludeConsumptionTypes = KnowledgeDataComputing.mapping.obj("excludeByConsumptionTypes").arr.mkString(",").replaceAll("\\\"", "").split(",")

        originDF
          .where(
            not(col(PlaybacksColumns.consumptiontype).isin(excludeConsumptionTypes:_*))
          )
    }
  }
}
