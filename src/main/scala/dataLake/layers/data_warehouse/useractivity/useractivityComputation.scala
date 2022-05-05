package dataLake.layers.data_warehouse.useractivity

import dataLake.core.KnowledgeDataComputing
import dataLake.core.layers.ComputationInterface
import dataLake.layers.data_warehouse.playbacks.transformations.{AddPlaybacksColumnsTranformations, JoinPlaybacksAndCatalogue}
import org.apache.spark.sql.DataFrame

object useractivityComputation extends ComputationInterface {

  override def process(originDataFrames: Map[String, DataFrame]): DataFrame = {

    val entitlementsDF = originDataFrames("entitlements")
    val householdsDF = originDataFrames("households")

    val executionControl = KnowledgeDataComputing.executionControl


    //val playbacksCatalogueJoinedDF = .process()

    null
  }
}