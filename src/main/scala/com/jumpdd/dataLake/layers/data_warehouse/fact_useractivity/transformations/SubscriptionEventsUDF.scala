package com.jumpdd.dataLake.layers.data_warehouse.fact_useractivity.transformations

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable.ArrayBuffer

private case class Event(eventtype: String, event: String, status: String, producttype: String, suscriptionid: String, householdtype: String, addon: String, daydate: String)

object SubscriptionEventsUDF {

  private val eventStructType: ArrayType = ArrayType(
    new StructType()
      .add("eventtype", StringType)
      .add("event", StringType)
      .add("status", StringType)
      .add("producttype", StringType)
      .add("suscriptionid", StringType)
      .add("householdtype", StringType)
      .add("addon", StringType)
      .add("daydate", StringType)
  )

  val eventUDF: UserDefinedFunction = udf(
    (trialPeriod: String, startAt: String, endAt: String, productType: String, subscriptionId: String, householdType: String, addon: String) =>
      process(trialPeriod, startAt, endAt, productType, subscriptionId, householdType, addon), eventStructType)

  private def process(trialPeriod: String, startAt: String, endAt: String, productType: String, subscriptionId: String, householdType: String, addon: String): ArrayBuffer[Event] = {
    val events = ArrayBuffer[Event]()
    val trialPeriodToInt = trialPeriod.toInt
    // DATE FORMAT
    val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val finalFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    // ASK ALEX!!!
    //val startDate = format.parse(startAt)
    //val endDate = format.parse(endAt)
    //val startDateFormat = finalFormat.format(startAt)
    val startDateFormat = startAt.replace("-", "")
    val endDateSubscriptionFormat = endAt.replace("-", "")

    // CALENDAR
    val dateStartTrial = format.parse(startAt)
    val calendar = Calendar.getInstance()
    calendar.setTime(dateStartTrial)
    calendar.add(Calendar.DAY_OF_YEAR, trialPeriodToInt - 1)
    // TRIAL DAYS LEFT
    val endTrialStatusDate = finalFormat.format(calendar.getTime)
    // CURRENT date
    val currentDate = finalFormat.format(Calendar.getInstance().getTime)


    // TYPE OF SUSCRIPTION
    var startStatusSubscriptionValue = "PayingSubscription"

    if (trialPeriodToInt != 0) {
      startStatusSubscriptionValue = "TrialSubscription"
    }

    // START SUSCRIPTION EVENT
    events += Event("Start", "Subscription", startStatusSubscriptionValue, productType, subscriptionId, householdType, addon, startDateFormat)
    // STATE CHANGE EVENT
    events += Event("StateChange", startStatusSubscriptionValue, startStatusSubscriptionValue, productType, subscriptionId, householdType, addon, startDateFormat)
    // PRODUCTTYPE EVENT
    events += Event("ProductTypeChange", productType, startStatusSubscriptionValue, productType, subscriptionId, householdType, addon, startDateFormat)

    // CHECK WITH CURRENT DATE TO DECIDE YOU NEED ADD EVENT TO STATECHANGE TRIAL TO PAYING
    if ((trialPeriodToInt != 0 && endTrialStatusDate < currentDate) && (trialPeriodToInt != 0 && endTrialStatusDate < endDateSubscriptionFormat) || (trialPeriodToInt != 0 && endAt.isEmpty && endTrialStatusDate < currentDate)) {
      events += Event("StateChange", "PayingSubscription", "PayingSubscription", productType, subscriptionId, householdType, addon, endTrialStatusDate)
      events += Event("Start", "Subscription", "PayingSubscription", productType, subscriptionId, householdType, addon, endTrialStatusDate)
    }

    // SUSPENSION EVENT-
    // END SUSCRIPTION EVENT
    if ((endAt.nonEmpty && endTrialStatusDate < currentDate) && (endAt.nonEmpty && endTrialStatusDate < endDateSubscriptionFormat)) {

      events += Event("End", "Subscription", "PayingSubscription", productType, subscriptionId, householdType, addon, endTrialStatusDate)
    } else if ((endAt.nonEmpty && endTrialStatusDate >= currentDate) || (endAt.nonEmpty && endTrialStatusDate >= endDateSubscriptionFormat) && (endTrialStatusDate <= currentDate && endTrialStatusDate >= endDateSubscriptionFormat)) {
      events += Event("End", "Subscription", "TrialSubscription", productType, subscriptionId, householdType, addon, endTrialStatusDate)
    }

    events
  }
}
