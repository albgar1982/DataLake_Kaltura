package com.jumptvs.etls.events

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import com.jumptvs.etls.model.Global
import com.jumptvs.etls.model.m7.M7
import com.jumptvs.etls.utils.DateUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object PayingSubscriptionEventUDF extends BaseEvent {

  override val eventName: String = "trialEventType"

  private def getPackageEvent(subscriptionType: String): String = {

    subscriptionType match {
      case "BUN" => "Bundle"
      case "STD" => "Standalone"
      case _ => "Standalone"
    }
  }

  private def payingSubscriptionEventType(startDate: Option[String],
                                          endDate: Option[String],
                                          subscriptionType: String,
                                          status: String,
                                          subscriptionTrialDays: String): Seq[Event] = {



    val (start, end) = (startDate.getOrElse(""), endDate.getOrElse(""))
    val packageName = getPackageEvent(subscriptionType)
    val trialDays = subscriptionTrialDays.toInt
    val startDateLocalDate = DateUtils.convertStringToLocalDateTime(start, M7.dateFormat)
    val startString = DateUtils.convertLocalDateTimeToString(startDateLocalDate, M7.dateFormat)
    val trialDate = DateUtils.addDaysToDateTime(start, trialDays.toInt, M7.dateFormat)
    val trialDateString = DateUtils.convertLocalDateTimeToString(trialDate, M7.dateFormat)
    val endDateLocalDate = if (end.eq("")) startDateLocalDate else DateUtils.convertStringToLocalDateTime(end, M7.dateFormat)
    val endString = DateUtils.convertLocalDateTimeToString(endDateLocalDate, M7.dateFormat)
    val daysBetweenStartDateAndTrialEndDate = ChronoUnit.DAYS.between(startDateLocalDate, LocalDateTime.now())
    val daysBetweenStartDateAndEndDate = ChronoUnit.DAYS.between(startDateLocalDate, endDateLocalDate)


      (trialDays, start, end, daysBetweenStartDateAndTrialEndDate, daysBetweenStartDateAndEndDate) match {

      case active_TrialSubscription if (end.isEmpty & (trialDays > 0) && start.nonEmpty && (daysBetweenStartDateAndTrialEndDate <= trialDays)) => {
        Seq(
          Event("StateChange", "TrialSubscription", "TrialSubscription", false, startString), // A
          Event("Start", "Subscription", "TrialSubscription", false,  startString), // A
          Event("ProductTypeChange", packageName, "TrialSubscription", false, startString) // A - 1
        )}
      case active_Trial_PayingSubscription if (end.isEmpty & (trialDays > 0) && start.nonEmpty && (daysBetweenStartDateAndTrialEndDate > trialDays)) => {
        Seq(
          Event("StateChange", "PayingSubscription", "PayingSubscription", false, trialDateString), // B - 1
          Event("StateChange", "TrialSubscription", "TrialSubscription", false, startString), // A1
          Event("Start", "Subscription", "TrialSubscription", false, startString), // A
          Event("Start", "Subscription", "PayingSubscription", false, trialDateString), // B - 1
          Event("ProductTypeChange", packageName, "TrialSubscription", false, start) // A - 2
        )}
      case active_PayingSubscription if (end.isEmpty & !(trialDays > 0) && start.nonEmpty) => {

        Seq(
          Event("StateChange", "PayingSubscription", "PayingSubscription", false, startString), // B - 2
          Event("Start", "Subscription", "PayingSubscription", false,  startString), // C - 2
          Event("ProductTypeChange", packageName, "PayingSubscription", false, startString) // E
        )}
      case cancelled_Trial_PayingSubscription if ((trialDays > 0) && end.nonEmpty && (daysBetweenStartDateAndEndDate > trialDays)) => {
        Seq(
          Event("End", "Subscription", "PayingSubscription", false,  endString), // D
          Event("StateChange", "PayingSubscription", "PayingSubscription", false, trialDateString), // B - 1
          Event("StateChange", "TrialSubscription", "TrialSubscription", false, startString), // A
          Event("Start", "Subscription", "TrialSubscription", false,  startString), // A
          Event("Start", "Subscription", "PayingSubscription", false, trialDateString), // B - 1
          Event("ProductTypeChange", packageName, "TrialSubscription", false, startString) // A - 1
        )}
      case cancelled_TrialSubscription if ((trialDays > 0) && end.nonEmpty && (daysBetweenStartDateAndEndDate <= trialDays)) => {
        Seq(
          Event("End", "Subscription", "TrialSubscription", false,  endString),
          Event("StateChange", "TrialSubscription", "TrialSubscription", false, startString), // A
          Event("Start", "Subscription", "TrialSubscription", false,  startString), // A
          Event("ProductTypeChange", packageName, "TrialSubscription", false, startString) // A - 1
        )}
      case cancelled_PayingSubscription if (!(trialDays > 0) && end.nonEmpty) => {
        Seq(
          Event("End", "Subscription", "PayingSubscription", false,  endString), // D
          Event("StateChange", "PayingSubscription", "PayingSubscription", false, startString), // B - 2
          Event("Start", "Subscription", "PayingSubscription", false,  startString), // C - 2
          Event("ProductTypeChange", packageName, "PayingSubscription", false, startString) // E
        )}
      case _ => {
        Seq(Event(Global.naColName, Global.naColName, Global.naColName, false, Global.naColName)
        )}
     }
  }

  override val eventUDF: UserDefinedFunction = udf(
    (startDate: String, endDate: String, subscriptionType: String, status: String, subscriptionTrialDays: String) =>
      payingSubscriptionEventType(Option(startDate), Option(endDate), subscriptionType, status, subscriptionTrialDays), eventStructType)
}