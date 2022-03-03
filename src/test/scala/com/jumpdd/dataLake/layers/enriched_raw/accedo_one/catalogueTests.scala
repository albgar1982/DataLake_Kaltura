package com.jumpdd.dataLake.layers.enriched_raw.accedo_one

import dataLake.core.{DateRange, ExecutionControl, KnowledgeDataComputing, Launcher, Spark}
import org.apache.spark.sql.SaveMode
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should

import java.time.LocalDate

class catalogueTests extends AnyFunSuite with should.Matchers with BeforeAndAfter {

  var args: Array[String] = _

  before {
      args = Array(
        "--brandId", "5f2fc318-d0b0-11ea-aeb0-07543f0a7b99",
        "--environment", "dev",
        "--flavour", "product",
        "--knowledgeUrl", "localhost:XXXX",
        "--layer", "enriched_raw",
        "--executionGroup", "accedo",
        "--table", "catalogue",
        "--config", "accedo",
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

  test("Delta Execution") {
    KnowledgeDataComputing.isFull = false
    Launcher.main(args)
  }

  test("convert csv to parquet") {
    val spark = Spark.configureSparkSession()
    val pathReadParquet = "seeds/raw/accedo_one/playbacks"
    val readParquetFile = "seeds/raw/accedo_one/playbacks"

    val pathWriteParquet = "seeds/raw/playbacksessions"
    val pathReadCsv = "seeds/samples/seeds_kaltura_fake/raw_playbacksessions.csv"
    val pathWriteCsv = " "

    val df = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .csv(pathReadCsv)

    df.show(false)
    df.printSchema()

    df.write.mode(SaveMode.Overwrite).parquet(pathWriteParquet)
  }

  test("step-by-step implementation") {
    KnowledgeDataComputing.isFull = false

    val spark = Spark.configureSparkSession()

    val catalogueDF = spark.read.parquet("seeds/mpp/address")
    /*
    //val catalogueDF = spark.read.parquet("seeds/raw/accedo_one/catalogue")
    val catalogueDF = spark.read.parquet("seeds/fake_raw/accedo_one/catalogue")

    println(catalogueDF
      .schema.json)

    val showDF = CatalogueFilterAndEnrichInputDataTransformation.process(catalogueDF)

    showDF.show(false)
    showDF.printSchema()

     */

    //showDF.write.mode(SaveMode.Overwrite).parquet("results/fake_enriched_raw/accedo_one/catalogue")

  }

}
