package dataLake.layers.enriched_raw.kaltura.entitlements.transformations

import dataLake.core.layers.UsersColumns.daydate
import dataLake.layers.enriched_raw.kaltura.entitlements.entitlementsModel.{cancellation_date, cancellation_reason, create_date, household_id, module_id, rn, subscriptionid, update_date}
import dataLake.layers.enriched_raw.kaltura.playbacksessions.transformations.FilterAndEnrichInputDataTransformation.{cleanEntitlementsRaw, cleanPlaybackSessionsRaw, joinEntitlementsAndPlaybackSessions}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}

object FilterAndEnrichInputEntitlementsTransformation {
  def process(entitlementsDF: DataFrame): DataFrame = {

    val entitlementsCleanDF = cleanEntitlements(entitlementsDF)
    entitlementsCleanDF


  }

  def cleanEntitlements (entitlementsDF: DataFrame): DataFrame = {

    val w1 = Window.partitionBy(household_id, module_id, create_date).orderBy(col(daydate).asc)
    val startSubDF = entitlementsDF
      .filter(col(household_id).isNotNull)
      .withColumn(rn, row_number().over(w1))
      .where(col(rn) === 1)
      .drop(rn)

    val selectedColumnsDF = startSubDF.select(household_id, module_id, create_date,
      cancellation_date,cancellation_reason)

    selectedColumnsDF


  }





}
