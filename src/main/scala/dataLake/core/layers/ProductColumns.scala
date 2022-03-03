package dataLake.core.layers

object PlaybacksColumns {
  val brandid = "brandid"
  val daydate = "daydate"
  val yeardate = "yeardate"
  val monthdate = "monthdate"
  val hourdate = "hourdate"
  val seconddate = "seconddate"

  val userid = "userid"
  val countrycode = "countrycode"
  val regioncode = "regioncode"
  val device = "device"
  val devicetype = "devicetype"

  val subscriptionid = "subscriptionid"
  val producttype = "producttype"
  val householdtype = "householdtype"
  val operator = "operator"

  val contentid = "contentid"
  val serieid = "serieid"
  val episode = "episode"
  val season = "season"
  val title = "title"
  val serietitle = "serietitle"
  val fulltitle = "fulltitle"
  val contentduration = "contentduration"
  val duration = "duration"

  val playbacktime = "playbacktime"

  val genres = "genres"
  val network = "network"
  val consumptiontype = "consumptiontype"
  val contenttype = "contenttype"
  val channel = "channel"

  val deviceinfo = "deviceinfo"
  val synopsis = "synopsis"

  //ADD MISSING COLUMNS FROM THE PLAYBACKS DOCUMENTATION
  val origin = "origin"
  val viewerid = "viewerid"
  val customerid = "customerid"
  val bufferingtime = "bufferingtime"
  val broadcasting = "broadcasting"
  val channelid = "channelid"
  val typeItem = "type"
  val bitrate = "bitrate"

}

object UsersColumns {
  val brandid = "brandid"
  val daydate = "daydate"
  val hourdate = "hourdate"
  val monthdate = "monthdate"
  val yeardate = "yeardate"

  val activationdaydate = "activationdaydate"
  val deactivationdaydate = "deactivationdaydate"

  val status = "status"
  val event = "event"
  val eventtype = "eventtype"
  val cancellationtype = "cancellationtype"

  val userid = "userid"
  val countrycode = "countrycode"
  val country = "country"
  val regioncode = "regioncode"
  val region = "region"
  val gender = "gender"
  val birth = "birth"
  val email = "email"
  val city = "city"
  val origin = "origin"

  val subscriptionid = "subscriptionid"
  val producttype = "producttype"
  val paymentmethod = "paymentmethod"
  val householdtype = "householdtype"
  val addon = "addon"
  val operator = "operator"

  val accountid = "accountid"
  val serviceid = "serviceid"
  val servicetitle = "servicetitle"
  val startdate = "startdate"
  val cancellationdate = "cancellationdate"
  val serviceexpiry = "serviceexpiry"

}
