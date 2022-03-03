package dataLake.core.layers.data_warehouse.registereduser

import dataLake.core.layers.ComputationInterface
import dataLake.core.layers.data_warehouse.playbackseriesactivity.transformations.{FilterContentsSeriesTransformation, SetContentTypeNameTransformation}
import dataLake.core.layers.data_warehouse.registereduser.transformations.SnapshotTransformation
import org.apache.spark.sql.DataFrame

object registereduserComputation extends ComputationInterface {

  override def process(originDataFrames: Map[String, DataFrame]): DataFrame = {

    val userActivityDF = originDataFrames("useractivity")
    SnapshotTransformation.process(userActivityDF)

  }
}