package dataLake.layers.data_warehouse.playbacks.transformations

import dataLake.core.ExecutionControl
import dataLake.layers.data_warehouse.playbacks.playbacksModel._
import dataLake.core.layers.PlaybacksColumns.{channel, consumptiontype, daydate, device, devicetype, hourdate, householdtype, monthdate, operator, yeardate}
import dataLake.layers.enriched_raw.kaltura.catalogue.catalogueModel.media_type
import dataLake.layers.enriched_raw.kaltura.playbacksessions.playbacksessionsModel.{device_brand_name, device_family_name, device_id, device_id_total, device_udid, epg_id, epg_identifier, media_id, session_end_reason, start_timestamp, user_id}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, lit, substring, when}

object AddPlaybacksColumnsTranformations {

  def process(originDF: DataFrame): DataFrame = {

    val deviceAndDatesDF = addDeviceAndDates(originDF)
    val cleanColumns = cleanRawColumns(deviceAndDatesDF)

    cleanColumns

  }


  def cleanRawColumns(originDF: DataFrame): DataFrame = {

     val cleanedDF = originDF.drop(col(device_id))
        .drop(col(session_end_reason))
        .drop(col(media_id))
        .drop(col(device_udid))
        .drop(col(user_id))
        .drop(col(epg_identifier))
        .drop(col(epg_id))
        .drop(col(media_type))
        .drop(col(device_family_name))
        .drop(col(device_brand_name))
        .drop(col(device_id_total))

      cleanedDF

  }

  def addDeviceAndDates(originDF: DataFrame): DataFrame = {
    val createColumnsPlayback = originDF
      .withColumn(devicetype,
        when(col(device_family_name) === "Smart Phone", lit("1"))
          .when(col(device_family_name) === "Game Consoles", lit("6"))
          .when(col(device_family_name) === "STB", lit("2"))
          .when(col(device_family_name) === "PC/MAC", lit("5"))
          .otherwise(lit("0"))
      )
      .withColumn(device,
        when(col(device_brand_name) === "Samsung Galaxy S", lit("2"))
          .when(col(device_brand_name) === "XBox", lit("24"))
          .when(col(device_brand_name) === "Kaon", lit("34"))
          .when(col(device_brand_name) === "PC/MAC", lit("22"))
          .when(col(device_brand_name) === "iPhone", lit("1"))
          .when(col(device_brand_name) === "Android", lit("31"))
          .otherwise(lit("0"))
      )
      .withColumn(origin, lit("DIALOG"))
      .withColumn(channel, col("epg_identifier"))
      .withColumn(consumptiontype, col("media_type"))
      .withColumn(operator, lit("NA"))
      .withColumn(householdtype, lit("NA"))
      .withColumn(daydate,
        concat(
          substring(col(start_timestamp), 1, 4),
          substring(col(start_timestamp), 6, 2),
          substring(col(start_timestamp), 9, 2)
        )
      )
      .withColumn(monthdate,
        concat(
          substring(col(start_timestamp), 1, 4),
          substring(col(start_timestamp), 6, 2),
          lit("01")
        )
      )
      .withColumn(yeardate,
        concat(
          substring(col(start_timestamp), 1, 4),
          lit("0101")
        )
      )
      .withColumn(hourdate,
        concat(
          substring(col(start_timestamp), 1, 4),
          substring(col(start_timestamp), 6, 2),
          substring(col(start_timestamp), 9, 2),
          substring(col(start_timestamp), 12, 2)
        )
      )

    val catchupConsumptiontype = createColumnsPlayback.withColumn(consumptiontypenew,
      when(col(consumptiontype).equalTo("epg"), lit("CATCHUP")).otherwise(col(consumptiontype)))


    catchupConsumptiontype

  }

}
