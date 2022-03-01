package com.jumpdd.dataLake.layers.data_warehouse

import com.jumpdd.dataLake.core.Spark
import com.jumpdd.dataLake.{DateRange, ExecutionControl, KnowledgeDataComputing, Launcher}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should
import com.jumpdd.dataLake.core.layers.ModelInterface
import org.apache.spark.sql.functions.lit

import java.time.LocalDate

class playbackActivityTests extends AnyFunSuite with should.Matchers with BeforeAndAfter {

  var args: Array[String] = _

  before {
    args = Array(
      "--brandId", "285df710-355f-11eb-9917-e71a95af48ce",
      "--environment", "dev",
      "--flavour", "product",
      "--knowledgeUrl", "localhost:XXXX",
      "--layer", "data_warehouse",
      "--executionGroup", "playbacks",
      "--table", "playbackactivity"
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



  }

}
