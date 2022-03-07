package dataLake.layers.enriched_raw.kaltura.households.transformations

import dataLake.core.layers.UsersColumns.daydate
import dataLake.layers.enriched_raw.kaltura.households.householdsModel._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}

object FilterAndEnrichInputHouseholdsTransformation {
  def process(householdsDF: DataFrame): DataFrame = {

    val householdsCleanDF = cleanHouseHold(householdsDF)

    householdsCleanDF


  }

  def cleanHouseHold (householdsDF: DataFrame): DataFrame = {

    val w0 = Window.partitionBy(household_id).orderBy(col("daydate").asc)

    val startRegDF = householdsDF
      .filter(col(action).notEqual("DELETED"))
      .withColumn(rn, row_number().over(w0))
      .where(col(rn) === 1)
      .drop(rn)

    val selectedColumnsDF = startRegDF.select(create_date,household_id,subscriptionid,update_date)
    selectedColumnsDF

  }


}
