package dataLake.core.layers.data_warehouse.playbackseriesactivity

import dataLake.core.layers.ComputationInterface
import dataLake.core.layers.data_warehouse.playbackseriesactivity.transformations.{FilterContentsSeriesTransformation, SetContentTypeNameTransformation}
import org.apache.spark.sql.DataFrame

object playbackseriesactivityComputation extends ComputationInterface {

  override def process(originDataFrames: Map[String, DataFrame]): DataFrame = {

    val playbacksDF = originDataFrames("playbackactivity")

    val filteredDF = FilterContentsSeriesTransformation.process(playbacksDF)
    SetContentTypeNameTransformation.process(filteredDF)
  }
}