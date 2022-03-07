package dataLake.layers.data_warehouse.useractivity.transformations

import dataLake.core.layers.UsersColumns.{cancellationtype, daydate, event, eventtype, producttype, status, userid}
import dataLake.layers.data_warehouse.useractivity.useractivityModel.rn
import dataLake.layers.enriched_raw.kaltura.entitlements.entitlementsModel.{cancellation_date, cancellation_reason, create_date, end_date, household_id, module_id, module_type, subscriptionid}
import dataLake.layers.enriched_raw.kaltura.playbacksessions.playbacksessionsModel.module_name
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, concat, lit, row_number, substring, when}

object SubscriptionEventsTransformations {


  def generateSuscription(entitlementsDF: DataFrame): DataFrame = {

    val createSubscriptionsDF = createSubscription(entitlementsDF)
    val createStateChangeDF = stateChange(createSubscriptionsDF)
    val productTypeChangeDF = productType(createSubscriptionsDF)

    val totalSubscriptionDF = createSubscriptionsDF
      .union(createStateChangeDF)
      .union(productTypeChangeDF)

    val endSubscriptionDF = endSubscription(entitlementsDF)

    val subscriptionUserDF = totalSubscriptionDF.union(endSubscriptionDF)

    subscriptionUserDF.show(30,false)

    subscriptionUserDF



  }
  def createSubscription(entitlementsDF: DataFrame): DataFrame = {

    val w1 = Window.partitionBy(household_id, module_id, create_date).orderBy(col(daydate).asc)
    val startSubDF = entitlementsDF
      .filter(col(household_id).isNotNull)
      .withColumn(rn, row_number().over(w1))
      .where(col(rn) === 1)
      .drop(rn)
      .withColumn(eventtype, lit("Start"))
      .withColumn(event, lit("Subscription"))
      .withColumn(status, lit("PayingSubscription"))
      .withColumn(daydate, concat(
        substring(col(create_date), 1, 4),
        substring(col(create_date), 6, 2),
        substring(col(create_date), 9, 2)
      ))
      .withColumn(cancellationtype, lit("NA"))
      .withColumn(producttype, col("module_type"))
      .withColumnRenamed(household_id, userid)
      .withColumnRenamed(module_name, subscriptionid)
      .select(userid, status, eventtype, event, cancellationtype, producttype, subscriptionid, daydate)

    startSubDF
  }

  def stateChange(subscriptionDF: DataFrame): DataFrame = {

    subscriptionDF
      .withColumn(eventtype, lit("StateChange"))
      .withColumn(event, lit("PayingSubscription"))
      .withColumn(status, lit("PayingSubscription"))
      .withColumn(cancellationtype, lit("NA"))

  }

  def productType(subscriptionDF: DataFrame): DataFrame = {

    val w1 = Window.partitionBy(userid, producttype).orderBy(col(daydate).asc)

    val productTypeChangeDF = subscriptionDF
      .filter(col(userid).isNotNull)
      .withColumn(rn, row_number().over(w1))
      .where(col(rn) === 1)
      .drop(rn)
      .withColumn(eventtype, lit("ProductTypeChange"))
      .withColumn(event, col("producttype"))
      .withColumn(status, lit("PayingSubscription"))
      .withColumn(cancellationtype, lit("NA"))

    productTypeChangeDF

  }


  def endSubscription(entitlementsDF: DataFrame): DataFrame = {

    val w1 = Window.partitionBy(household_id, module_id, create_date).orderBy(col(daydate).asc)
    val endSubDFUser = entitlementsDF.filter(col(household_id).isNotNull)
      .filter(col(status).equalTo(lit("INACTIVE")))
      .withColumn(rn, row_number().over(w1))
      .where(col(rn) === 1)
      .drop(rn)
      .withColumn(eventtype, lit("End"))
      .withColumn(event, lit("Subscription"))
      .withColumn(status, lit("PayingSubscription"))
      .withColumn(daydate, concat(
        substring(col(end_date), 1, 4),
        substring(col(end_date), 6, 2),
        substring(col(end_date), 9, 2)
      ))
      .withColumn(cancellationtype, lit("User"))
      .withColumn(producttype, col("module_type"))
      .withColumnRenamed(household_id, "userid")
      .withColumnRenamed(module_name, "subscriptionid")
      .select(userid, status, eventtype, event, cancellationtype, producttype, subscriptionid, daydate)

    val endSubDFService=  entitlementsDF.filter(col(household_id).isNotNull)
      .filter(col(cancellation_reason).notEqual(lit("NA")))
      .withColumn(rn, row_number().over(w1))
      .where(col(rn) === 1)
      .drop(rn)
      .withColumn(eventtype, lit("End"))
      .withColumn(event, lit("Subscription"))
      .withColumn(status, lit("PayingSubscription"))
      .withColumn(daydate, concat(
        substring(col(cancellation_date), 1, 4),
        substring(col(cancellation_date), 6, 2),
        substring(col(cancellation_date), 9, 2)
      ))
      .withColumn(cancellationtype,
        when(col(cancellation_reason).equalTo(lit("FORCE CANCEL")),lit("User")).otherwise(lit("Service"))
      )
      .withColumn(producttype, col(module_type))
      .withColumnRenamed(household_id, userid)
      .withColumnRenamed(module_name, subscriptionid)
      .select(userid, status, eventtype, event, cancellationtype, producttype, subscriptionid, daydate)

    val totalEndSubDF= endSubDFUser.union(endSubDFService).dropDuplicates()

    totalEndSubDF
  }



}


