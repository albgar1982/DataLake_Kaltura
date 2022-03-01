package com.jumpdd.dataLake.layers.enriched_raw.playbacks.transformations

import com.jumpdd.dataLake.core.layers.PlaybacksColumns.{brandid, daydate}
import com.jumpdd.dataLake.layers.enriched_raw.playbacks.playbacksColumns.{city, country, device_type, region, time, video, video_percent_viewed, video_seconds_viewed, viewer}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, current_date, explode, lit, regexp_replace}

object PlaybackFilterAndEnrichInputDataTransformation {
  def process(originDF: DataFrame): DataFrame = {


    val pruebaDF = originDF.select(explode(col("items"))).select("col.country",
      "col.video_seconds_viewed","col.city","col.viewer", "col.device_type",
      "col.video","col.video_percent_viewed","col.time","col.region"
    )


    val enrichedDF = pruebaDF
      .withColumnRenamed("viewer",viewer)
      .withColumnRenamed("country",country)
      .withColumnRenamed("video_seconds_viewed",video_seconds_viewed)
      .withColumnRenamed("city",city)
      .withColumnRenamed("device_type",device_type)
      .withColumnRenamed("video",video)
      .withColumnRenamed("video_percent_viewed",video_percent_viewed)
      .withColumnRenamed("time",time)
      .withColumnRenamed("region",region)

    // TODO change daydate in dwh to  time

    val partitioncolumnsDF = enrichedDF.withColumn(brandid,lit("xxx"))
      .withColumn(daydate, regexp_replace(current_date(),"-",""
      ))

    partitioncolumnsDF

  }
}
