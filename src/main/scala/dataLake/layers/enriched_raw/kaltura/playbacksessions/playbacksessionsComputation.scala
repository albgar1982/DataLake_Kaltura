package dataLake.layers.enriched_raw.kaltura.playbacksessions

import dataLake.core.layers.ComputationInterface
import dataLake.layers.enriched_raw.kaltura.playbacksessions.transformations.FilterAndEnrichInputDataTransformation
import org.apache.spark.sql.DataFrame

object playbacksessionsComputation extends ComputationInterface {

  override def process(originDataFrames: Map[String, DataFrame]): DataFrame = {

    val rawplaybackDF = originDataFrames("playbacksessions")
    val rawentitlementsDF = originDataFrames("entitlements")


    val filteredAndEnrichedDF = FilterAndEnrichInputDataTransformation.process(rawplaybackDF, rawentitlementsDF)

    filteredAndEnrichedDF
  }

}
