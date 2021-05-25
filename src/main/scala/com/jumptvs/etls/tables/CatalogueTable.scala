package com.jumptvs.etls.tables

import com.jumptvs.etls.model.Global

object CatalogueTable extends Tables {

  val finalColumns: Seq[String] = Seq(
    Global.customeridColName,
    Global.genresColName,
    Global.networkColName,
    Global.contentidColName,
    Global.titleColName,
    Global.serietitleColName,
    Global.episodeColName,
    Global.seasonColName,
    Global.fulltitleColName,
    Global.contenttypeColName,
    Global.contentdurationColName,
    Global.countrycodeColName,
    Global.groupcontenttypeColName,
    Global.channelColName,
    Global.brandidColName,
    Global.daydateColName
  )

}
