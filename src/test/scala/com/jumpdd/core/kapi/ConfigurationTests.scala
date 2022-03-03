package com.jumpdd.core.kapi

import dataLake.core.{DateRange, ExecutionControl, KnowledgeDataComputing, Launcher, Spark}
import org.apache.spark.sql.SaveMode
import org.scalatest.{Assertions, BeforeAndAfter}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should

import java.time.LocalDate

class ConfigurationTests extends AnyFunSuite with should.Matchers with BeforeAndAfter {

  var args: Array[String] = _

  before {
      args = Array(
        "--brandId", "5f2fc318-d0b0-11ea-aeb0-07543f0a7b99",
        "--kapiConfigUrl", "http://54.229.31.189:31112",
        "--kapiConfigToken", "read_access_token",
        "--environment", "local",
        "--flavour", "default",
        "--branch", "default",
        "--layer", "enriched_raw",
        "--executionGroup", "accedo",
        "--table", "catalogue",
        "--remote", "false"
      )

    val dateRange = DateRange(LocalDate.now().minusMonths(1), LocalDate.now())

    KnowledgeDataComputing.maxMonthsToCompute = 24
    KnowledgeDataComputing.executionControl = new ExecutionControl(dateRange.start, dateRange.end)
  }

  test("Full Execution") {
    KnowledgeDataComputing.isFull = true
    Launcher.main(args)
  }

  test("Step by step") {



    val spark = Spark.configureSparkSession(false)

    //val rawDF = spark.read.parquet(pathReadParquet).filter(col("daydate") === "20220103")

    val selectedColumns = Seq("countrycode","regioncode","postalcode","")
    val postalcodeColumnName = "PostCode"

    //val resultDF = enrichmentPostalCodeColumns(null,postalcodeColumnName,selectedColumns)

    val df = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .csv("seeds/samples/dwh/useractivity.csv")
    df.show(false)
    df.printSchema()
    df.write.mode(SaveMode.Overwrite).parquet("results/data_warehouse/fact_useractivity")

    //resultDF.show(10)


  }
}
