package com.jumptvs.etls.model.m7

import com.jumptvs.etls.model.{Global, Tmp}
import org.apache.spark.sql.types.{DoubleType, LongType}

object Facts {
  val renamedUserActivityColumns: Map[String, String] = Map(
    Global.subscriberidColName -> M7.userId,
    Global.producttypeColName -> M7.productType,
    Global.subscriptionidColName -> M7.subscriptionPackageName,
    Global.countrycodeColName -> M7.country,
    Global.birthColName -> M7.internalBirthday,
    Global.regioncodeColName -> M7.region,
    Global.eventtypeColName -> Tmp.jumpEventType,
    Global.eventColName -> Tmp.jumpEvent,
    Global.massivedeactivationColName -> Tmp.jumpMassiveDeactivation,
    Global.statusColName -> Tmp.jumpStatus
  )

  val addNewUserActivityColumns: Map[String, Any] = Map(
    Global.billedamountColName -> ("0.0", DoubleType),
    Global.billingcurrencyColName -> Global.naColName,
    Global.periodColName -> ("0", LongType),
    Global.cityColName -> Global.naColName,
    Global.neighborhoodColName -> Global.naColName,
    Global.zipcodeColName -> Global.naColName,
    Global.genderinferredColName -> Global.naColName,
    Global.businessunitColName -> Global.naColName,
    Global.operatorColName -> Global.naColName,
    Global.originColName -> "M7",
    Global.customeridColName -> "af6f35ba-5f82-11ea-b6ba-0354e9fb4247"
  )

  val renamedPlaybackActivityColumns: Map[String, String] = Map(
    Global.subscriberidColName -> "userid",
    Global.secondDateColName -> "internalseconddate",
    Global.playbacktimeColName -> "internalplaybacktime"
  )

  val addNewPlaybacksColumns: Map[String, Any] = Map(
    Global.typeColName -> "PlayButtonClicked",
    Global.operatorColName -> Global.naColumn,
    Global.originColName -> "Accedo One",
    Global.regioncodeColName -> Global.naColumn,
    Global.customeridColName -> "0ef7c99a-355f-11eb-9916-2b51b3ca4c38",
    Global.subscriptionidColName -> Global.naColumn,
    Global.producttypeColName -> Global.naColumn
  )

  val renamedBillingColumns: Map[String, String] = Map(
    Global.subscriberidColName -> M7.userId,
    Global.producttypeColName -> M7.productType,
    Global.amountsubscribercurrencyColName -> M7.amountTotal,
    Global.amountpublishercurrencyColName -> M7.amountTotal,
    Global.subscribercurrencyColName -> M7.currency,
    Global.publishercurrencyColName -> M7.currency,
    Global.paymentmethodColName -> M7.paymentMethodName,
    Global.countrycodeColName -> M7.country,
    Global.regioncodeColName -> M7.region,
    Global.subscriptionidColName -> M7.subscriptionPackageName
  )

  val addNewBillingColumns: Map[String, String] = Map(
  )

  val renamedDimSubscriptionsColumns: Map[String, String] = Map(
    Global.countrycodeColName -> M7.country,
    Global.subscriptionidColName -> M7.subscriptionPackageName
  )

  val addNewDimSubscriptionsColumns: Map[String, Any] = Map(
    Global.producttypeColName -> "Standalone"
  )

  val renamedCatalogueColumns: Map[String, String] = Map(
    Global.genresColName -> M7.genres,
    Global.networkColName -> M7.owner,
    Global.contentdurationColName -> M7.duration,
    Global.contentidColName -> M7.externalId
  )

  val addNewCatalogueColumns: Map[String, String] = Map(
  )

  val renamedEpgColumns: Map[String, String] = Map(
    Global.titleColName -> M7.programTitle,
    Global.fulltitleColName -> M7.programTitle,
    Global.contentdurationColName -> M7.durationSeconds,
    Global.channelColName -> M7.channelNameEPG,
    Global.broadCastingTimeColName -> M7.internalBroadcastDatetime
  )

  val addNewEpgColumns: Map[String, Any] = Map(
    Global.genresColName -> Global.naColName,
    Global.networkColName -> Global.naColName,
    Global.contentidColName -> Global.naColName,
    Global.serietitleColName -> Global.naColName,
    Global.episodeColName -> Global.naColName,
    Global.seasonColName -> Global.naColName,
    Global.contenttypeColName -> Global.naColName,
    Global.episodeTitleColName -> Global.naColName,
    Global.synopsisColName -> Global.naColName,
    Global.channelIdColName -> Global.naColName
  )
}
