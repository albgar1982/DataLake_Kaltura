package dataLake.layers.enriched_raw

import dataLake.core.{DateRange, ExecutionControl, KnowledgeDataComputing, Launcher, Spark}
import org.apache.spark.sql.SaveMode
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should

import java.time.LocalDate

class playbacksessionsTest extends AnyFunSuite with should.Matchers with BeforeAndAfter {

  var args: Array[String] = _

  before {
    args = Array(
      "--brandId", "285df710-355f-11eb-9917-e71a95af48ce",
      "--environment", "dev",
      "--flavour", "product",
      "--knowledgeUrl", "localhost:XXXX",
      "--layer", "enriched_raw",
      "--executionGroup", "kaltura",
      "--table", "playbacksessions"
    )

    val dateRange = DateRange(LocalDate.now().minusMonths(1), LocalDate.now())

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

    val pathReadCsv = "seeds/samples/seeds_kaltura_fake/raw_playbacksessions.csv"
    val pathWriteParquet = "seeds/raw/playbacksessions"

    val spark = Spark.configureSparkSession()
    val df = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .csv(pathReadCsv)
    df.show(false)
    df.printSchema()
    df.write.partitionBy("brandid","daydate").mode(SaveMode.Overwrite).parquet(pathWriteParquet)

    //TODO change current_date() in daydate
  }

}
