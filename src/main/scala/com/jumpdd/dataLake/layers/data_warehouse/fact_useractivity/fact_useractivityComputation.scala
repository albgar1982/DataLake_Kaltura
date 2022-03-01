package com.jumpdd.dataLake.layers.data_warehouse.fact_useractivity

import com.jumpdd.dataLake.KnowledgeDataComputing
import com.jumpdd.dataLake.core.layers.{ComputationInterface, UsersColumns}
import com.jumpdd.dataLake.layers.data_warehouse.fact_useractivity.transformations.{CalculateDaysAndFormatDatesTransformation, CreateUseractivityColumnsTransformation, FillNaTransformations, SubscriptionEventsUDF}
import com.jumpdd.dataLake.layers.enriched_raw.entitlements.entitlementsColumns
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, from_unixtime, lit, substring, unix_timestamp}
import org.apache.spark.sql.types._

object fact_useractivityComputation extends ComputationInterface {

  override def process(originDataFrames: Map[String, DataFrame]): DataFrame = {

    val executionControl = KnowledgeDataComputing.executionControl

    val entitlementsDF = originDataFrames("entitlements")



    entitlementsDF




    }

}
