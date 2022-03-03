package dataLake.core.layers.data_warehouse.playbackseriesactivity.transformations

import dataLake.core.KnowledgeDataComputing
import dataLake.core.layers.PlaybacksColumns
import dataLake.core.utilities.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, not}

object SetContentTypeNameTransformation extends Logger {

  def process(originDF: DataFrame): DataFrame = {
    originDF.withColumn(PlaybacksColumns.contenttype, lit(KnowledgeDataComputing.mapping.obj("contentTypeValueOfSerie").str))
  }
}
