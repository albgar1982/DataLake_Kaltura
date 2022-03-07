package dataLake.layers.data_warehouse.playbacks.transformations

import dataLake.core.layers.PlaybacksColumns.{brandid, channel, consumptiontype, contentduration, contentid, contenttype, countrycode, daydate, device, devicetype, episode, fulltitle, hourdate, monthdate, operator, playbacktime, producttype, regioncode, season, subscriptionid, title, userid, viewerid, yeardate}
import dataLake.layers.data_warehouse.playbacks.playbacksModel._
import dataLake.layers.enriched_raw.kaltura.playbacksessions.playbacksessionsModel.{device_external_id, household_id, module_name, module_type}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, sum}


  object CastAndRenameColumns {

    def process(originDF: DataFrame): DataFrame = {

      val factPlaybackActivity = originDF.withColumn(device, col(device).cast("Long"))
        .withColumn(devicetype, col(devicetype).cast("Long"))
        .withColumn(userid, col(household_id))
        .drop(col(household_id))




      val EnrichedRawPlaybackSessions = factPlaybackActivity
        .groupBy(
          col(userid),
          col(device_external_id),
          col(module_name),
          col(module_type),
          col(hourdate),
          col(monthdate),
          col(yeardate),
          col(countrycode),
          col(regioncode),
          col(contentid),
          col(consumptiontypenew),
          col(title),
          col(channel),
          col(serietitle),
          col(season),
          col(episode),
          col(fulltitle),
          col(genres),
          col(contentduration),
          col(origin),
          col(operator),
          col(devicetype),
          col(device),
          col(contenttype),
          col(network),
          col(brandid),
          col(daydate))
        .agg(sum(col(playbacktime)).alias(playbacktime))
        .select(
          col(userid),
          col(device_external_id).alias(viewerid),
          col(module_name).alias(subscriptionid),
          col(module_type).alias(producttype),
          col(hourdate),
          col(monthdate),
          col(yeardate),
          col(countrycode),
          col(regioncode),
          col(contentid),
          col(consumptiontypenew).alias(consumptiontype),
          col(title),
          col(channel),
          col(serietitle),
          col(season),
          col(episode),
          col(fulltitle),
          col(genres),
          col(playbacktime).cast("Long"),
          col(contentduration).cast("Long"),
          col(origin),
          col(operator),
          col(devicetype),
          col(device),
          col(contenttype),
          col(network),
          col(brandid),
          col(daydate)
        )


        EnrichedRawPlaybackSessions


    }

}
