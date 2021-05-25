package com.jumptvs.etls.model.m7.sources

trait Epg extends DimSubscriptions {
  val channelNameEPG = "channel_name"
  val programTitle = "program_title"
  val broadcastDatetime = "broadcast_datetime"
  val durationSeconds = "duration_seconds"
  override val country = "country"
  val stationId = "station_id"
  val internalBroadcastDatetime = "internal_broadcast_datetime"
}
