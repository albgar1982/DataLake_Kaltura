package com.jumpdd.dataLake.layers.data_warehouse.playbackactivity

import com.jumpdd.dataLake.KnowledgeDataComputing
import com.jumpdd.dataLake.core.layers.PlaybacksColumns.{daydate, genres}
import com.jumpdd.dataLake.core.layers.{ComputationInterface, PlaybacksColumns, UsersColumns}
import com.jumpdd.dataLake.core.spark.DataFrameFactory
import com.jumpdd.dataLake.layers.data_warehouse.fact_catalogue.transformations.CreateFactCatalogueColumnsTransformation
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.{col, from_unixtime, lit, lower, rand, regexp_replace, substring, substring_index, udf, when}
import com.jumpdd.dataLake.core.layers.{ComputationInterface, UsersColumns}
import com.jumpdd.dataLake.layers.data_warehouse.playbackactivity.playbackactivityModel.{internal_hourdate, internal_timestamp}
import com.jumpdd.dataLake.layers.data_warehouse.playbackactivity.transformations.PlaybacksColumnsTransformations
import com.jumpdd.dataLake.layers.enriched_raw.brightcove.playbacks
import com.jumpdd.dataLake.layers.enriched_raw.playbacks.playbacksColumns

import scala.util.Random

object playbackactivityComputation extends ComputationInterface {

  override def process(originDataFrames: Map[String, DataFrame]): DataFrame = {

    val catalogueDF = originDataFrames("fact_catalogue")
    val playbacksDF = originDataFrames("playbacks")

    val executionControl = KnowledgeDataComputing.executionControl


    catalogueDF.show()
    playbacksDF.show()
    playbacksDF
  }
}