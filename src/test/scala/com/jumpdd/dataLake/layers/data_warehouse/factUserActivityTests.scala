package com.jumpdd.dataLake.layers.data_warehouse

import cats.implicits.toShow
import com.jumpdd.dataLake.core.Spark
import com.jumpdd.dataLake.layers.data_warehouse.fact_catalogue.transformations.{CreateFactCatalogueColumnsTransformation, DragContentsBetweenDatesTransformation, PrepareCMSDataTransformation}
import com.jumpdd.dataLake.layers.enriched_raw.entitlements.entitlementsColumns.{end_date, start_date}
import com.jumpdd.dataLake.layers.enriched_raw.beacon.users.usersColumns
import com.jumpdd.dataLake.layers.enriched_raw.entitlements.entitlementsColumns
import com.jumpdd.dataLake.{DateRange, ExecutionControl, KnowledgeDataComputing, Launcher}
import org.apache.spark.sql.catalyst.plans.logical.Window
import org.apache.spark.sql.expressions.Window.partitionBy
import org.apache.spark.sql.functions.{col, from_unixtime, lit, row_number}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

import java.time.LocalDate
import scala.util.parsing.json.JSON.flatten4

class factUserActivityTests extends AnyFunSuite with should.Matchers with BeforeAndAfter {

  var args: Array[String] = _

  before {
    args = Array(
      "--brandId", "285df710-355f-11eb-9917-e71a95af48ce",
      "--environment", "dev",
      "--flavour", "product",
      "--knowledgeUrl", "localhost:XXXX",
      "--layer", "data_warehouse",
      "--executionGroup", "subscriptions",
      "--table", "fact_useractivity"
    )

    val dateRange = DateRange(LocalDate.now().minusMonths(1), LocalDate.now())

    KnowledgeDataComputing.maxMonthsToCompute = 24
    KnowledgeDataComputing.executionControl = new ExecutionControl(dateRange.start, dateRange.end)
  }

  test("Full Execution") {
    KnowledgeDataComputing.isFull = true
    Launcher.main(args)
  }

  test("Delta Execution") {
    KnowledgeDataComputing.isFull = false
    Launcher.main(args)
  }

  test("step-by-step implementation") {
    KnowledgeDataComputing.isFull = false

    val spark = Spark.configureSparkSession()

    // TODO obsolete data - to be remove

    val entitlementsDF = spark.read.parquet("results/enriched_raw/beacon/entitlements")
    val usersDF = spark.read.parquet("results/enriched_raw/beacon/users")

    val dateDF = entitlementsDF.withColumn("internalstart_date",from_unixtime(col(start_date) / 1000, "yyyy/MM/dd HH:mm:ss"))
      .withColumn("internalend_date",from_unixtime(col(end_date)/1000, "yyyy/MM/dd HH:mm:ss"))



   val idDF= usersDF.withColumn("new_column",lit("ABC"))

  }

}
