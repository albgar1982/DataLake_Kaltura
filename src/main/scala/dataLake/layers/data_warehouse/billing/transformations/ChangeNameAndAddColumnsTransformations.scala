package dataLake.layers.data_warehouse.billing.transformations

import dataLake.core.layers.PlaybacksColumns.{brandid, consumptiontype, contentduration, contentid, contenttype, countrycode, daydate, episode, fulltitle, regioncode, season, serietitle, title}
import dataLake.core.layers.UsersColumns.{monthdate, paymentmethod, producttype, servicetitle, subscriptionid, userid}
import dataLake.layers.data_warehouse.billing.billingModel.{amountpublishercurrency, amountsubscribercurrency, publishercurrency, subscribercurrency}
import dataLake.layers.data_warehouse.catalogue.catalogueModel.{genreposition, providerposition}
import dataLake.layers.enriched_raw.kaltura.billing.billingModel.{billing_state, catalog_currency_code, catalog_price, create_date, household_id, module_name, module_type, paid, payment_method, transaction_currency_code}
import dataLake.layers.enriched_raw.kaltura.catalogue.catalogueModel.{assetDuration, catalog_start, identifier, media_id, media_type, meta_name, meta_name_value, name}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array_contains, array_position, col, concat, element_at, lit, substring, when}

object ChangeNameAndAddColumnsTransformations {
  def process(originDF: DataFrame): DataFrame = {

    val changeNameDF = addDatesAndChangeColumnsNames(originDF)
    val factbilling = selectFinalColumns(changeNameDF)

    factbilling
  }


  def addDatesAndChangeColumnsNames(originDF: DataFrame): DataFrame = {

    val billingDF = originDF
      .withColumnRenamed(module_name, subscriptionid)
      .withColumnRenamed(module_type, producttype)
      .withColumn(catalog_price, when(col(catalog_price).equalTo("NA"), lit("NA"))
        .otherwise(col(catalog_price)))
      .withColumn(paymentmethod, when(col(payment_method).isNull, lit("NA")).otherwise(col(payment_method)))
      .withColumn(amountpublishercurrency, col(catalog_price))
      .withColumn(amountsubscribercurrency, col(paid))
      .withColumn(publishercurrency, col(catalog_currency_code))
      .withColumn(subscribercurrency, col(transaction_currency_code))
      .withColumn(daydate,
        concat(
          substring(col(create_date), 1, 4),
          substring(col(create_date), 6, 2),
          substring(col(create_date), 9, 2)
        )
      )
      .withColumn(monthdate,
        concat(
          substring(col(create_date), 1, 4),
          substring(col(create_date), 6, 2),
          lit("01")
        )
      )

    billingDF

  }

  def selectFinalColumns(originDF: DataFrame): DataFrame = {

    val finalDF = originDF
      .select(
        lit("b750c112-bd42-11eb-b0e4-f79ca0b77da6").alias(brandid),
        col(daydate),
        col(monthdate),
        lit("NA").alias(countrycode),
        lit("NA").alias(regioncode),
        col(household_id).alias(userid),
        col(producttype),
        col(subscriptionid),
        col(paymentmethod),
        col(amountpublishercurrency).cast("Double"),
        col(amountsubscribercurrency).cast("Double"),
        col(publishercurrency),
        col(subscribercurrency)
      ).dropDuplicates()
      .filter(col(subscriptionid).isNotNull)

    finalDF


  }
}