package com.jumpdd.dataLake.layers.enriched_raw.cms.transformations

import com.jumpdd.dataLake.layers.cms.cmsColumns._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, regexp_replace}

object FilterAndEnrichInputDataTransformation {
  def process(originDF: DataFrame): DataFrame = {
    originDF
      .select(id, name, duration, custom_fields, published_at, state, updated_at, brandid, dateColumns.daydate)
      .withColumn(internalgenres, regexp_replace(col("custom_fields.beacon_genre"), ", ", ","))
      .withColumn(internalserietitle, col("custom_fields.beacon_episode_seriename"))
      .withColumn(internalseason, col("custom_fields.beacon_episode_seasonnumber"))
      .withColumn(internalepisode, col("custom_fields.beacon_episode_number"))

  }
}
