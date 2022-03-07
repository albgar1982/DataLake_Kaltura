package dataLake.layers.data_warehouse.useractivity.transformations

import dataLake.core.layers.UsersColumns.{cancellationtype, daydate, event, eventtype, producttype, status, subscriptionid, userid}
import dataLake.layers.enriched_raw.kaltura.billing.billingModel.household_id
import dataLake.layers.enriched_raw.kaltura.households.householdsModel.{action, create_date, rn, update_date}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, concat, lit, row_number, substring}

object RegistrationEventsTransformations {



  def generateRegistration(householdsDF: DataFrame,entitlementsDF: DataFrame): DataFrame = {

    val startRegistrationDF = createRegistration(householdsDF)
    val endRegistrationDF = endRegistration(householdsDF)

    val registrationUserDF = startRegistrationDF.union(endRegistrationDF)

    registrationUserDF.show(30, false)
    registrationUserDF

  }

  def createRegistration(householdsDF: DataFrame): DataFrame = {

    val w0 = Window.partitionBy("household_id").orderBy(col("daydate").asc)

    val startRegDF = householdsDF
      .filter(col(action).notEqual("DELETED"))
      .withColumn(rn, row_number().over(w0))
      .where(col(rn) === 1)
      .drop(rn)
      .withColumn(eventtype, lit("Start"))
      .withColumn(event, lit("Registration"))
      .withColumn(status, lit("NA"))
      .withColumn(cancellationtype, lit("NA"))
      .withColumn(producttype, lit("NA"))
      .withColumn(daydate, concat(
        substring(col(create_date), 1, 4),
        substring(col(create_date), 6, 2),
        substring(col(create_date), 9, 2)
      ))
      .withColumnRenamed(household_id, userid)
      .withColumn(subscriptionid, lit("NA"))
      .select(userid, status, eventtype, event, cancellationtype, producttype, subscriptionid, "daydate")

    startRegDF
  }

  def endRegistration(householdsDF: DataFrame): DataFrame = {

    val w0 = Window.partitionBy(household_id).orderBy(col(daydate).asc)

    val endRegDF = householdsDF
      .filter(col(action).equalTo("DELETED"))
      .withColumn(rn, row_number().over(w0))
      .where(col(rn) === 1)
      .drop(rn)
      .withColumn(eventtype, lit("End"))
      .withColumn(event, lit("Registration"))
      .withColumn(status, lit("NA"))
      .withColumn(cancellationtype, lit("NA"))
      .withColumn(producttype, lit("NA"))
      .withColumn(daydate, concat(
        substring(col(update_date), 1, 4),
        substring(col(update_date), 6, 2),
        substring(col(update_date), 9, 2)
      ))
      .withColumnRenamed(household_id, userid)
      .withColumn(subscriptionid, lit("NA"))
      .select(userid, status, eventtype, event, cancellationtype, producttype, subscriptionid, daydate)

    endRegDF

  }

}
