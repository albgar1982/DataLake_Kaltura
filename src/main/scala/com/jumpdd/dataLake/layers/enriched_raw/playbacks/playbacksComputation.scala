package com.jumpdd.dataLake.layers.enriched_raw.playbacks

import cats.implicits.toShow
import com.jumpdd.dataLake.core.layers.ComputationInterface
import com.jumpdd.dataLake.core.layers.PlaybacksColumns.{brandid, daydate}
import com.jumpdd.dataLake.layers.cms.cmsColumns
import com.jumpdd.dataLake.layers.enriched_raw.brightcove.playbacks.playbacksColumns.{city, country, device_type, region, time, video, video_percent_viewed, video_seconds_viewed, viewer}
import com.jumpdd.dataLake.layers.enriched_raw.playbacks.transformations.PlaybackFilterAndEnrichInputDataTransformation
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, current_date, explode, lit, regexp_replace, substring_index}


object playbacksComputation extends ComputationInterface {

  override def process(originDataFrames: Map[String, DataFrame]): DataFrame = {

    val playbacksDF = originDataFrames.get("playbacks").get

    val enrichedDF = PlaybackFilterAndEnrichInputDataTransformation.process(playbacksDF)

    enrichedDF


  }
}
