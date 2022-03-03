package dataLake.core.layers.data_warehouse.registereduser.transformations

import dataLake.core.layers.UsersColumns
import dataLake.core.layers.UsersColumns.{daydate, subscriptionid, userid}
import dataLake.core.layers.data_warehouse.registereduser.registereduserModel.{eventdate, valuetype}
import dataLake.core.utilities.Logger
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{DataFrame, functions}

object DataPreparationTransformation extends Logger {

  def process(originDF: DataFrame): DataFrame = {

    val activationsDF = originDF
      .where(col(UsersColumns.eventtype).isin("Start", "StateChange", "ProductTypeChange"))
      .withColumn(valuetype,
        when(col(UsersColumns.event) === lit("Registration"), 1)
          .when(col(UsersColumns.event) === lit("Subscription"), 2)
          .when(col(UsersColumns.eventtype) === lit("StateChange"), 2)
          .when(col(UsersColumns.eventtype) === lit("ProductTypeChange"), 2)
      )

    val deactivationsDF = originDF
      .where(col(UsersColumns.eventtype).equalTo("End"))
      .withColumn(valuetype,
        when(col(UsersColumns.event) === lit("Registration"), 1)
          .when(col(UsersColumns.event) === lit("Subscription"), 2)
      ).withColumn(daydate, col(daydate))
      .withColumn(UsersColumns.cancellationtype, col(UsersColumns.cancellationtype))

    val joinedDF = activationsDF
      .join(
        deactivationsDF,
        activationsDF.col(subscriptionid).equalTo(deactivationsDF.col(subscriptionid))
          .and(activationsDF.col(userid).equalTo(deactivationsDF.col(userid)))
          .and(activationsDF.col(daydate).leq(deactivationsDF.col(daydate)))
          .and(activationsDF.col(valuetype).equalTo(deactivationsDF.col(valuetype)))
        ,"left_outer"
      ).select(
        activationsDF.col(UsersColumns.userid),
        activationsDF.col(daydate).alias(UsersColumns.activationdaydate),
        deactivationsDF.col(daydate).alias(UsersColumns.deactivationdaydate),
        activationsDF.col(UsersColumns.eventtype),
        activationsDF.col(UsersColumns.status),
        deactivationsDF.col(UsersColumns.cancellationtype),
        activationsDF.col(valuetype),
        activationsDF.col(UsersColumns.producttype),
        activationsDF.col(subscriptionid),
        activationsDF.col(UsersColumns.paymentmethod),
        activationsDF.col(UsersColumns.countrycode),
        activationsDF.col(UsersColumns.country),
        activationsDF.col(UsersColumns.regioncode),
        activationsDF.col(UsersColumns.region),
        activationsDF.col(UsersColumns.operator),
        //activationsDF.col(UsersColumns.householdtype),
        //activationsDF.col(UsersColumns.addon),
        activationsDF.col(UsersColumns.email),
        activationsDF.col(UsersColumns.birth),
        activationsDF.col(UsersColumns.gender)
      )

    val groupedDF = joinedDF
      .groupBy(
        UsersColumns.paymentmethod, UsersColumns.activationdaydate, UsersColumns.userid,
        UsersColumns.eventtype, UsersColumns.status, valuetype, UsersColumns.cancellationtype,

        UsersColumns.operator, UsersColumns.email,
        //UsersColumns.householdtype, UsersColumns.addon,
        UsersColumns.producttype, subscriptionid,
        UsersColumns.countrycode, UsersColumns.country, UsersColumns.regioncode, UsersColumns.region,
      ).agg(
      functions.min(col(UsersColumns.deactivationdaydate)).alias(UsersColumns.deactivationdaydate)
    )

    val finalDF = groupedDF
      .withColumn(
        UsersColumns.deactivationdaydate,
        when(col(UsersColumns.deactivationdaydate).isNull, lit("NA")).otherwise(col(UsersColumns.deactivationdaydate))
      )

    finalDF
  }
}
