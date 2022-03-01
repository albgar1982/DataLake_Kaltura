package com.jumpdd.dataLake.layers.enriched_raw.playbacks

import com.jumpdd.dataLake.core.layers.ModelInterface


object playbacksColumns extends ModelInterface {

  val viewer = "viewer"
  val country = "country"
  val video_seconds_viewed = "video_seconds_viewed"
  val city = "city"
  val device_type ="device_type"
  val video = "video"
  val video_percent_viewed ="video_percent_viewed"
  val time = "time"
  val region = "region"

  // val video_name = "video.name"
  // val video_view = "video_view"
  // val video_impression = "video_impression"
  // val play_request = "play_request"


}
