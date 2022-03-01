package com.jumpdd.dataLake.layers.data_warehouse

import com.jumpdd.dataLake.{DateRange, ExecutionControl, KnowledgeDataComputing, Launcher}
import com.jumpdd.dataLake.core.{ArgumentsConfig, Spark}
import com.jumpdd.dataLake.layers.data_warehouse.fact_catalogue.fact_catalogueComputation
import com.jumpdd.dataLake.layers.data_warehouse.fact_catalogue.transformations.{CreateFactCatalogueColumnsTransformation, DragContentsBetweenDatesTransformation, PrepareCMSDataTransformation}
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDate

class factCatalogueTests extends AnyFunSuite with should.Matchers with BeforeAndAfter {

  var args: Array[String] = _

  before {
    args = Array(
      "--brandId", "285df710-355f-11eb-9917-e71a95af48ce",
      "--environment", "dev",
      "--flavour", "product",
      "--knowledgeUrl", "localhost:XXXX",
      "--layer", "data_warehouse",
      "--executionGroup", "playbacks",
      "--table", "fact_catalogue"
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

      val modifiedCmsDF = PrepareCMSDataTransformation.calc(cmsDF, executionControl)
      modifiedCmsDF.show()

      val tableColumnsDF = CreateFactCatalogueColumnsTransformation.calc(modifiedCmsDF, executionControl)

      val finalDF = DragContentsBetweenDatesTransformation.calc(tableColumnsDF, executionControl)
      finalDF
        .show()

    }catch{
      case t: Throwable => t.printStackTrace(System.out)
        print(t)
    }


  }

}
