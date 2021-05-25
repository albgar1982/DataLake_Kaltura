package com.jumptvs.etls.facts.factPlaybackActivity

import com.jumptvs.etls.config.GlobalConfiguration
import com.jumptvs.etls.facts.BaseLoadExecution
import com.jumptvs.etls.model.Global.naColumn
import com.jumptvs.etls.model.m7.M7
import com.jumptvs.etls.model.{Global, MappingContenTypes, Tmp}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

case class Input(playbacksDF: DataFrame, frontendUsersDF: DataFrame, catalogueDF: DataFrame, epgDF: DataFrame, registerdDF: DataFrame)
object Raw extends BaseLoadExecution[Input, DataFrame] {

  private val defaultPlaybacksColumns = Seq(
    M7.userIdPlayback,
    Tmp.monotonically_unique_id,
    M7.playSubscription,
    M7.assetOwner,
    M7.assetID,
    M7.assetName,
    M7.assetType,
    M7.playOfferId,
    M7.playStartTime,
    M7.internalPlayStartTime,
    M7.playDurationExPause,
    M7.assetDuration,
    M7.communityName,
    M7.deviceMainType,
    M7.deviceSubType,
    M7.playSessionID,
    M7.internalPlayStartTimeStr,
    M7.daydate,
    M7.genres,
    M7.seriesTitle,
    M7.season,
    M7.owner,
    M7.episode,
    M7.seriesExternalid,
    Global.channelColName
  )
  def playbacksJoinRegisteredUser(playbacksDF: DataFrame, groupedUsersAndSubscriptionDF: DataFrame): DataFrame = {

    val renamedDategroupedUsersAndSubscriptionDF = groupedUsersAndSubscriptionDF.withColumnRenamed(M7.daydate, M7.registeredUserDaydate)

    playbacksDF
      .join(renamedDategroupedUsersAndSubscriptionDF,
          (playbacksDF.col(M7.userIdPlayback) === renamedDategroupedUsersAndSubscriptionDF.col(Tmp.userId)) &&
          (playbacksDF.col(M7.daydate) === renamedDategroupedUsersAndSubscriptionDF.col(M7.registeredUserDaydate)), "left")
      .orderBy(col(M7.activationDaydate).asc, col(M7.deactivationDaydate).desc, col(Global.subscriptionidColName).asc)
      .dropDuplicates(Tmp.monotonically_unique_id)
      .drop(Tmp.monotonically_unique_id)
  }

  def process(input: Input, config: GlobalConfiguration): DataFrame = {

    val playbacksUniquesDF =  input.playbacksDF.withColumn(Tmp.monotonically_unique_id,org.apache.spark.sql.functions.monotonically_increasing_id())

    val playbackWithCatalogueDF = crossPlaybackWithVODCatalogueDF(playbacksUniquesDF, input.catalogueDF)
    val playbackWithEpgDF = crossPlaybacksWithEPG(playbacksUniquesDF, input.epgDF)
    val playbacksEnrichedDF = playbackWithCatalogueDF.union(playbackWithEpgDF)

    val reducedBridgeDF = filterStreamgroupUser(input)

    val usersAndSubscriptionDF = getSubscribersUsers(reducedBridgeDF, input.registerdDF)

    playbacksJoinRegisteredUser(playbacksEnrichedDF,usersAndSubscriptionDF)
  }

  private def filterStreamgroupUser(input: Input): DataFrame = {

    val playUniqueUserIdDF =  input.playbacksDF.select(M7.userIdPlayback).dropDuplicates()
    val frontenUniqueUserIdDF = input.frontendUsersDF.withColumnRenamed(M7.userIdPlayback, Tmp.userId).dropDuplicates(Tmp.userId)

    playUniqueUserIdDF.join(frontenUniqueUserIdDF, playUniqueUserIdDF(M7.userIdPlayback)===frontenUniqueUserIdDF(Tmp.userId))
      .select(frontenUniqueUserIdDF(Tmp.userId), frontenUniqueUserIdDF(M7.internalName))

  }

  private def crossPlaybackWithVODCatalogueDF(playbacksDF: DataFrame, catalogueDF: DataFrame): DataFrame = {
    val filteredCatalogue = catalogueDF
      .select(M7.seriesTitle, M7.genres, M7.season, M7.owner, M7.episode, M7.externalId, M7.daydate, M7.title)
      .withColumn(Global.channelColName, naColumn)
      .withColumn(M7.seriesExternalid, lit(null))
      .orderBy(col(M7.daydate).desc)
      .dropDuplicates(M7.externalId)
      .drop(M7.daydate)

    val filteredPlaybacks = playbacksDF
      .filter(col(M7.assetType).isin(MappingContenTypes.broadcast, MappingContenTypes.restart, MappingContenTypes.replay, MappingContenTypes.npvr) === false)

    val crossDF = filteredPlaybacks.join(filteredCatalogue, playbacksDF.col(M7.assetID) === filteredCatalogue.col(M7.externalId), "left")
      .withColumn(M7.assetName, when(col(M7.title).isNotNull, col(M7.title)).otherwise(col(M7.assetName)))
      .withColumn(M7.owner, when(col(M7.owner).isNotNull, col(M7.owner)).otherwise(col(M7.assetOwner)))

    crossDF.select(defaultPlaybacksColumns map col: _*)
  }

  private def crossPlaybacksWithEPG(playbacksDF: DataFrame, epgDF: DataFrame): DataFrame = {

    val filteredPlaybacks = playbacksDF
      .filter(col(M7.assetType).isin(MappingContenTypes.broadcast, MappingContenTypes.restart, MappingContenTypes.replay, MappingContenTypes.npvr))

    val epgCleanedDF = epgDF
        .select(M7.channelNameEPG, M7.programTitle, M7.stationId, M7.internalBroadcastDatetime,M7.seriesTitle, Global.formatsColName,
          M7.durationSeconds, M7.genres, M7.season, M7.episode, M7.seriesExternalid)
        .withColumn(Global.channelColName, col(M7.channelNameEPG))
        .withColumn(Tmp.endProgram, from_unixtime(unix_timestamp(epgDF.col(M7.internalBroadcastDatetime)) + epgDF.col(M7.durationSeconds)))
      .withColumn(M7.owner, naColumn)

    val programStart = epgCleanedDF(M7.internalBroadcastDatetime)
    val programEnd = epgCleanedDF(Tmp.endProgram)
    val playbackStart = filteredPlaybacks(M7.internalPlayStartTimeCrossEpg)

    val inProgram = programStart <= playbackStart && playbackStart < programEnd
    val programId = epgCleanedDF(M7.stationId).equalTo(filteredPlaybacks(M7.assetID))

    val filter = inProgram && programId

    val crossDF = filteredPlaybacks.join(broadcast(epgCleanedDF), filter, "left")

    val finalDF = crossDF
      .withColumn(M7.assetName,   when(col(M7.programTitle).isNotNull, col(M7.programTitle)).otherwise(col(M7.assetName)))
      .withColumn(M7.assetDuration, when(col(M7.durationSeconds).isNotNull, col(M7.durationSeconds)).otherwise(lit("0")))
      .withColumn(Global.channelColName, when(col(M7.channelNameEPG).isNotNull, col(M7.channelNameEPG)).otherwise(col(M7.assetName)))
      .withColumn(M7.owner, when(col(Global.channelColName).isNotNull, col(Global.channelColName)).otherwise(col(M7.assetName)))
      .withColumn(M7.genres, when(col(M7.genres).isNotNull, col(M7.genres))
        .otherwise(col(Global.formatsColName)))

    finalDF.select(defaultPlaybacksColumns map col: _*)
  }


  private def getSubscribersUsers(frontendUsersDF: DataFrame, registeredDF: DataFrame): DataFrame = {
    frontendUsersDF
      .join(registeredDF, frontendUsersDF.col(M7.internalName) === registeredDF.col(M7.userId), "left")
  }
}
