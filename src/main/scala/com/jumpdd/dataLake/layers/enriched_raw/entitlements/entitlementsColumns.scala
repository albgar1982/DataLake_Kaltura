package com.jumpdd.dataLake.layers.enriched_raw.entitlements

import com.jumpdd.dataLake.core.layers.ModelInterface

object entitlementsColumns extends ModelInterface {

  val payment_method = "payment_method"
  val end_date = "end_date"
  val start_date = "start_date"
  val sku = "sku"
  val notification_type = "notification_type"
  val external_user_id = "external_user_id"
  val transaction_id = "transaction_id"
  val original_store = "original_store"
  val package_name = "package_name"
  val notification_date = "notification_date"
  val cancellation_date = "cancellation_date"
  val trial_end_date = "trial_end_date"


}
