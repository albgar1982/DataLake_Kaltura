package dataLake.layers.data_warehouse.playbacks

import dataLake.core.KnowledgeDataComputing
import dataLake.core.layers.{ComputationInterface, PlaybacksColumns}
import dataLake.layers.data_warehouse.playbacks.transformations.{AddPlaybacksColumnsTranformations, JoinPlaybacksAndCatalogue}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object playbacksComputation extends ComputationInterface {

  override def process(originDataFrames: Map[String, DataFrame]): DataFrame = {

    val catalogueDF = originDataFrames("fact_catalogue")
    val playbacksDF = originDataFrames("playbacks")

    val executionControl = KnowledgeDataComputing.executionControl


    val playbacksCatalogueJoinedDF = JoinPlaybacksAndCatalogue.process(playbacksDF,catalogueDF)

    val deviceAndDatesDF = AddPlaybacksColumnsTranformations.process(playbacksCatalogueJoinedDF)

    deviceAndDatesDF



  }
}