package dataLake.layers.data_warehouse.useractivity.transformations

import dataLake.core.layers.UsersColumns.yeardate
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, concat, lit, row_number, substring}

object FinalMappingTransformations {


  def createFinalMapping(registrationUserDF: DataFrame,subscriptionUserDF: DataFrame): DataFrame = {


    val totalActivityUser = registrationUserDF.union(subscriptionUserDF)


    val userActivityDF = totalActivityUser
      .withColumn("hourdate",
        concat(
          col("daydate"),
          lit("01")
        )
      )
      .withColumn("monthdate",
        concat(
          substring(col("daydate"), 1, 6),
          lit("01")
        )
      )
      .withColumn(yeardate,
        concat(
          substring(col("daydate"), 1, 4),
          lit("0101")
        )
      )

      .withColumn("brandid", lit("b750c112-bd42-11eb-b0e4-f79ca0b77da6"))
      .withColumn("origin", lit("Kaltura"))
      .withColumn("countrycode", lit("NA"))
      .withColumn("gender", lit("NA"))
      .withColumn("birth", lit("NA"))
      .withColumn("city", lit("NA"))
      .withColumn("regioncode", lit("NA"))
      .withColumn("neighborhood", lit("NA"))
      .withColumn("zipcode", lit("NA"))
      .withColumn("email", lit("NA"))
      .withColumn("genderinferred", lit("NA"))
      .withColumn("paymentmethod", lit("NA"))
      .withColumn("operator", lit("NA"))
      .select("brandid", "userid", "hourdate", "status", "eventtype", "event", "cancellationtype", "producttype",
        "daydate", "monthdate", "yeardate", "paymentmethod", "subscriptionid", "origin", "countrycode", "gender",
        "birth", "city", "regioncode", "neighborhood", "zipcode", "email", "genderinferred", "operator")

    userActivityDF
  }














}
