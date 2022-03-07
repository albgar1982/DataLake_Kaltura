package dataLake.layers.data_warehouse

import dataLake.core.{ExecutionControl, KnowledgeDataComputing, Launcher, Spark}
import dataLake.core.spark.DateRange
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should

import java.time.LocalDate

class userActivityTests extends AnyFunSuite with should.Matchers with BeforeAndAfter {

  var args: Array[String] = _

  before {
    args = Array(
      "--brandId", "285df710-355f-11eb-9917-e71a95af48ce",
      "--environment", "dev",
      "--flavour", "product",
      "--knowledgeUrl", "localhost:XXXX",
      "--layer", "data_warehouse",
      "--executionGroup", "playbacks",
      "--table", ""
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
    try{

      val spark = Spark.configureSparkSession()

      val cmsDF = spark.read.parquet("results/enriched_raw/brightcove/cms")
      val executionControl = KnowledgeDataComputing.executionControl



    }catch{
      case t: Throwable => t.printStackTrace(System.out)
        print(t)
    }