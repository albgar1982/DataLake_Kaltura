package com.jumpdd.dataLake.layers.enriched_raw.entitlements

import org.apache.spark.sql.DataFrame

object EntitlementsFilterAndEnrichInputDataTransformation {
  def process(originDF: DataFrame): DataFrame = {
    originDF
      .select(sku, start_date, end_date, notification_type, external_user_id, transaction_id, original_store, package_name, notification_date, cancellation_date, trial_end_date, brandid, dateColumns.daydate)

  }
}
