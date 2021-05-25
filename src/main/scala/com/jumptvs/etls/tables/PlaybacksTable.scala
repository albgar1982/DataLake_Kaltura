package com.jumptvs.etls.tables

import com.jumptvs.etls.model.Global

object PlaybacksTable extends Tables {

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
    Global.monthdateColName,
    Global.yeardateColName,
    Global.hourdateColName,
    Global.secondDateColName,
    Global.contentdurationColName,
    Global.countrycodeColName,
    Global.regioncodeColName,
    Global.brandidColName,
    Global.daydateColName,
    Global.devicetypeColName,
    Global.deviceColName,
    Global.playbacktimeColName,
    Global.subscriberidColName,
    Global.contenttypeColName,
    Global.groupcontenttypeColName,
    Global.originColName,
    Global.subscriptionidColName,
    Global.typeColName,
    Global.operatorColName,
    Global.channelColName,
    Global.producttypeColName
  )
}