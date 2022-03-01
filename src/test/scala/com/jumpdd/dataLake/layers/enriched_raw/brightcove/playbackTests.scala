package com.jumpdd.dataLake.layers.enriched_raw.brightcove

import com.jumpdd.dataLake.core.Spark
import com.jumpdd.dataLake.{DateRange, ExecutionControl, KnowledgeDataComputing, Launcher}
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.functions.{current_date, lit}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should

import java.time.LocalDate

class playbackTests extends AnyFunSuite with should.Matchers with BeforeAndAfter {

  var args: Array[String] = _

  before {
      args = Array(
        "--brandId", "285df710-355f-11eb-9917-e71a95af48ce",
        "--environment", "dev",
        "--flavour", "product",
        "--knowledgeUrl", "localhost:XXXX",
        "--layer", "enriched_raw",
        "--executionGroup", "brightcove",
        "--table", "playbacks"
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

    //TODO change current_date() in daydate

    val jsonDF = spark.read.json("seeds/response2.json/part-00000-e0e6cdfd-6e46-4021-9751-d2594c37b312-c000.json")
    //jsonDF.write.mode("overwrite").json("seeds/response2.json")

    val playDF = jsonDF.withColumn("brandid", lit("xxx")).withColumn("daydate",current_date())
    playDF.write.partitionBy("brandid","daydate").mode("overwrite").parquet("seeds/raw/brightcove/playbacks/playbacks.parquet")

    playDF.show(9)
    playDF


  }

}
