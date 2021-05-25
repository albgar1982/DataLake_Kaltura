package com.jumptvs.etls.model

import org.apache.spark.sql.functions.lit

object Global {

  val customeridColName = "customerid"
  val brandidColName = "brandid"
  val subscriberidColName = "subscriberid"
  val secondDateColName = "seconddate"
  val hourdateColName = "hourdate"
  val daydateColName = "daydate"
  val daydateTmpColName = "daydateTmp"
  val monthdateColName = "monthdate"
  val yeardateColName = "yeardate"
  val yearmonthColName = "yearmonth"
  val dayColName = "day"
  val monthColName = "month"
  val yearColName = "year"
  val eventtypeColName = "eventtype"
  val eventColName = "event"
  val statusColName = "status"
  val producttypeColName = "producttype"
  val subscriptionidColName = "subscriptionid"
  val billedamountColName = "billedamount"
  val billingcurrencyColName = "billingcurrency"
  val originColName = "origin"
  val periodColName = "period"
  val countrycodeColName = "countrycode"
  val massivedeactivationColName = "massivedeactivation"
  val genderColName = "gender"
  val birthColName = "birth"
  val cityColName = "city"
  val regioncodeColName = "regioncode"
  val operatorColName = "operator"
  val neighborhoodColName = "neighborhood"
  val zipcodeColName = "zipcode"
  val genderinferredColName = "genderinferred"
  val emailColName = "email"
  val businessunitColName = "businessunit"

  val contentidColName = "contentid"
  val titleColName = "title"
  val serietitleColName = "serietitle"
  val episodeColName = "episode"
  val seasonColName = "season"
  val fulltitleColName = "fulltitle"
  val contentdurationColName = "contentduration"
  val contenttypeColName = "contenttype"
  val genresColName = "genres"
  val networkColName = "network"

  val consumptionTypeColName = "consumptiontype"
  val playbacktimeColName = "playbacktime"
  val bufferingtimeColName = "bufferingtime"
  val bitrateColName = "bitrate"
  val deviceColName = "device"
  val devicetypeColName = "devicetype"
  val groupcontenttypeColName = "groupcontenttype"
  val typeColName = "type"

  val amountpublishercurrencyColName = "amountpublishercurrency"
  val publishercurrencyColName = "publishercurrency"
  val subscribercurrencyColName = "subscribercurrency"
  val amountsubscribercurrencyColName = "amountsubscribercurrency"
  val paymentmethodColName = "paymentmethod"

  val episodeTitleColName = "episodetitle"
  val synopsisColName = "synopsis"
  val channelIdColName = "channel_id"
  val broadCastingTimeColName = "broadcastingtime"
  val channelColName = "channel"

  val formatsColName = "formats"

  val naColName = "NA"
  val naColumn = lit("NA")

}

object Dates {

  val secondate = "seconddate"
  val hourdate = "hourdate"
  val daydate = "daydate"
  val monthdate = "monthdate"
  val yearDate = "yearDate"
  val yearMonth = "yearMonth"
  val day = "day"
  val month = "month"
  val year = "year"

  val yyyyMMddhhmmss = "yyyyMMddhhmmss"
  val yyyyMMddhh = "yyyyMMddhh"
  val yyyyMMddHH = "yyyyMMddHH"
  val yyyyMMdd = "yyyyMMdd"
  val yyyyMM = "yyyyMM"
  val yyyy = "yyyy"
  val yyyy_MM_dd = "yyyy-MM-dd"
  val yyyyMM01 = "yyyyMM01"
}

object Tmp {

  val jumpEventType = "jump_eventtype"
  val jumpEvent = "jump_event"
  val jumpStatus = "jump_status"
  val jumpMassiveDeactivation = "jump_massivedeactivation"
  val jumpDaydate = "jump_daydate"
  val jumpEventDate = "jump_eventdate"
  val JumpInputFileName = "JumpInputFileName"

  val userId = "tmpUserId"
  val frontUser = "tmpfrontUser"
  val count = "count"
  val row = "row"
  val inbetween = "inbetween"
  val numberOfPackages = "number_of_packages"
  val pack = "pack"
  val remove = "remove"
  val counter = "counter"
  val tmpDaydate = "tmpDaydate"
  val endProgram = "tmpEndProgram"

  val monotonically_unique_id = "monotonically_unique_id"
}

object MappingContenTypes {
  /** Value is m */
  val movie1 = "m";
  /** Value is t */
  val movie2 = "t";
  /** Value is s */
  val broadcast = "s";
  /** Value is q */
  val restart = "q";
  /** Value is r */
  val replay = "r";
  /** Value is v */
  val npvr = "v";
  /** Value is e */
  val episode = "e";
}