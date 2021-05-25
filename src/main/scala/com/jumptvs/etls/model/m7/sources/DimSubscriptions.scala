package com.jumptvs.etls.model.m7.sources

trait DimSubscriptions extends Catalog {
  override val country = "country"
  override val region = "region"
  val subscriptionPackageName = "subscription_package_name"
  val subscriptionPackageId = "subscription_package_id"
  val subscriptionPackageGroup = "subscription_package_group"
}
