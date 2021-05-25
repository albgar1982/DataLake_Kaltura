package com.jumptvs.etls.model.m7.sources

trait Catalog extends Billing {
  val externalId = "external_id"
  val title = "title"
  val owner = "owner"
  val seriesTitle = "series_title"
  val seriesExternalid = "series_externalid"
  val season = "season"
  val episode = "episode"
  val seriesEpisodeCount = "series_episode_count"
  val seriesLastEpisodeDate = "series_last_episode_date"
  val countries = "countries"
  val duration = "duration"
  val licenseStart = "license_start"
  val licenseEnd = "license_end"
  val genres = "Genres"
  val daydate = "daydate"
}
