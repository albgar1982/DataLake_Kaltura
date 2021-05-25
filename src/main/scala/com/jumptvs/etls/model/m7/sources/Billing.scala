package com.jumptvs.etls.model.m7.sources

trait Billing extends BackendUsers {
  override val country = "country"
  val region = "region"
  override val userId = "user_id"
  val transactionExpiryDatetime = "transaction_expiry_datetime"
  val currency = "currency"
  val billDatetime = "bill_datetime"
  val billCountryDatetime = "bill_country_datetime"
  val paymentDatetime = "payment_datetime"
  val paymentCountryDatetime = "payment_country_datetime"
  val amountBase = "amount_base"
  val amountTax = "amount_tax"
  val amountTotal = "amount_total"
  val paymentMethodName = "payment_method_name"
  val subscriptionID = "subscription_id"
}
