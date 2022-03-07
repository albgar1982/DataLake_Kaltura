package dataLake.layers.enriched_raw.kaltura.billing.transformations

import dataLake.core.layers.UsersColumns.daydate
import dataLake.layers.enriched_raw.kaltura.billing.billingModel.{billing_state, catalog_currency_code, catalog_price, module_name, module_type, paid, payment_method, transaction_currency_code}
import dataLake.layers.enriched_raw.kaltura.entitlements.entitlementsModel.{create_date, household_id, module_id, rn, subscriptionid, update_date}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col}

object FilterAndEnrichInputBillingTransformation {

  def process(entitlementsDF: DataFrame): DataFrame = {

    val entitlementsCleanDF = columnsRawBilling(entitlementsDF)
    entitlementsCleanDF


  }

  def columnsRawBilling (entitlementsDF: DataFrame): DataFrame = {


    val billingDF = entitlementsDF.where(
      col(module_type).equalTo("SUBSCRIPTION").and(col(billing_state).equalTo("SUCCESS"))
    ).select(module_name,module_type,billing_state,catalog_price,payment_method,paid,catalog_currency_code
      ,transaction_currency_code,create_date,daydate)

    billingDF

  }


}
