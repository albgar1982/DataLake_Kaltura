package com.jumptvs.etls.model.m7.sources

trait Subscribers extends Playbacks {
  override val userId = "user_id"
  val userId_tmp = "user_id_tmp"

  val subscriptionStartDatetime = "subscription_start_datetime"
  val subscriptionStartCountryDatetime = "subscription_start_country_datetime"
  val subscriptionTrialDays = "subscription_trial_days"
  val subscriptionCancellationDatetime = "subscription_cancellation_datetime"
  val subscriptionCancellationCountryDatetime = "subscription_cancellation_country_datetime"
  val internalSubscriptionStartDatetime = "internal_subscription_start_datetime"
  val internalSubscriptionStartCountryDatetime = "internal_subscription_start_country_datetime"
  val internalSubscriptionCancellationDatetime = "internal_subscription_cancellation_datetime"
  val internalSubscriptionCancellationCountryDatetime = "internal_subscription_cancellation_country_datetime"
  val subscriptionStatus = "subscription_status"
  override val country = "country"
  override val region = "region"
  override val subscriptionPackageName = "subscription_package_name"
  override val subscriptionPackageId = "subscription_package_id"
  val subscriptionPackageId_tmp = "subscription_package_id_tmp"
  val subscriptionType = "subscription_type"
  override val subscriptionID = "subscription_id"
  val subscriptionID_tmp = "subscription_id_tmp"
  val productType = "product_type"

  val internalSubscriptionStartDatetimeStr = "internal_subscription_start_datetime_str"
  val internalSubscriptionStartCountryDatetimeStr = "internal_subscription_start_country_datetime_str"
  val internalSubscriptionCancellationDatetimeStr = "internal_subscription_cancellation_datetime_str"

}
