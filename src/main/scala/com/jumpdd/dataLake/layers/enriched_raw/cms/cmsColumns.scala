package com.jumpdd.dataLake.layers.enriched_raw.cms

import com.jumpdd.dataLake.core.layers.{ModelInterface, UsersColumns}
import com.jumpdd.dataLake.core.layers.schema.ColumnObject
import com.jumpdd.dataLake.core.layers.schema.FormatTypes.{DAY_DATE, LOWER_CASE, USER_EVENT, USER_EVENT_TYPE, USER_STATUS}

/**
 *
 */

object cmsColumns extends ModelInterface {
  val id = "id"
  val name = "name"
  val duration = "duration"
  val custom_fields = "custom_fields"
  val published_at = "published_at"
  val state = "state"
  val updated_at = "updated_at"

  val internalgenres = "internalgenres"
  val internalserietitle = "internalserietitle"
  val internalseason = "internalseason"
  val internalepisode = "internalepisode"
}
