package dataLake.layers.enriched_raw.kaltura.playbacksessions.transformations

import dataLake.core.layers.PlaybacksColumns.playbacktime
import dataLake.core.layers.UsersColumns.{brandid, daydate}
import dataLake.layers.enriched_raw.kaltura.catalogue.catalogueModel.{assetDuration, catalog_start, identifier, media_id, media_type, meta_name, meta_name_value, name}
import dataLake.layers.enriched_raw.kaltura.playbacksessions.playbacksessionsModel.{device_external_id, device_id, device_id_total, device_udid, end_timestamp, household_id, module_name, module_type, session_end_reason, start_timestamp}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, regexp_replace, to_timestamp, unix_timestamp, when}

object FilterAndEnrichInputDataTransformation {

  def process(playbacksessionsDF: DataFrame,entitlementsDF: DataFrame): DataFrame = {

    val playbacksCleanDF = cleanPlaybackSessionsRaw(playbacksessionsDF)
    val entitlementsCleanDF = cleanEntitlementsRaw(entitlementsDF)

    val playbacksEnrichedDF = joinEntitlementsAndPlaybackSessions(playbacksCleanDF,entitlementsCleanDF)

    playbacksEnrichedDF


  }


  def joinEntitlementsAndPlaybackSessions(originDF: DataFrame,entitlementsDF: DataFrame): DataFrame = {

    val playbacksEnrichedDF = originDF.join(entitlementsDF, Seq(household_id, daydate), "left").dropDuplicates()
      .filter(col(household_id).isNotNull)


    playbacksEnrichedDF

  }


  def cleanPlaybackSessionsRaw(originDF: DataFrame): DataFrame = {

    val playbacksFields = originDF
      .filter(to_timestamp(col(start_timestamp),
        "yyyy-MM-dd HH:mm:ss") > "2021-12-20 00:00:00")

    val obtainPlaybacktime = playbacksFields
      .withColumn(device_id_total,
        when(col(device_id).isNull, lit(col(device_udid))
        ).otherwise(col(device_id)))
      .withColumn(device_external_id, regexp_replace(col(device_id_total), "\\^\\^\\^", "_"))
      .filter(col(session_end_reason) === "MEDIA_FINISHED" or col(session_end_reason) === "TERMINATED")
      .withColumn(playbacktime,
        (unix_timestamp(col(end_timestamp), "yyyy-MM-dd HH:mm:ss") - unix_timestamp(col(start_timestamp), "yyyy-MM-dd HH:mm:ss")) * 1000)
      .drop(col("start_media_position"))
      .drop(col("end_media_position"))
      .drop(col("business_model_type"))
      .drop(col("file_id"))
      .drop(col("business_model_id"))

    obtainPlaybacktime

  }


  def cleanEntitlementsRaw(originDF: DataFrame): DataFrame = {

    val entitlementsFields = originDF
      .filter(!col(module_name).contains("Mobile"))
      .select(col(household_id),
        col(module_type),
        col(module_name),
        col(daydate))
      .dropDuplicates()
      .drop(col(brandid))

    entitlementsFields
  }






}
