package dataLake.layers.enriched_raw.kaltura.catalogue

import dataLake.core.layers.ModelInterface

object catalogueModel extends ModelInterface {

  val media_id = "_media_id"
  val catalog_start = "basic.dates.catalog_start"
  val identifier = "basic.epg_identifier"
  val media_type = "basic.media_type"
  val name = "basic.name.value._VALUE"
  val assetDuration = "files.file._assetDuration"
  val meta_name = "structure.metas.meta._name"
  val meta_name_value = "structure.metas.meta.container"

}
