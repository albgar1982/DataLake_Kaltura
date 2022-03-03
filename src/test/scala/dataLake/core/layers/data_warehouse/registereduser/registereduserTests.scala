package dataLake.core.layers.data_warehouse.registereduser

import dataLake.core.KnowledgeDataComputing.sparkSession
import dataLake.core._
import dataLake.core.layers.UsersColumns
import dataLake.core.layers.data_warehouse.registereduser.transformations.{DataPreparationTransformation, SnapshotTransformation}
import org.apache.spark.sql.{SaveMode, functions}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should

import java.time.LocalDate

class registereduserTests extends AnyFunSuite with should.Matchers with BeforeAndAfter {

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

    val dateRange = DateRange(LocalDate.now().minusMonths(5), LocalDate.now())

    KnowledgeDataComputing.brandId = "5f2fc318-d0b0-11ea-aeb0-07543f0a7b99"
    KnowledgeDataComputing.maxMonthsToCompute = 24
    KnowledgeDataComputing.executionControl = new ExecutionControl(dateRange.start, dateRange.end)
    sparkSession = Spark.configureSparkSession()
  }

  test("Full Execution") {
    KnowledgeDataComputing.isFull = true
    Launcher.main(args)
  }

  test("Step by step parquet") {
    val userActivityDF = sparkSession.read.parquet("results/data_warehouse/useractivity")
    val preparedDF = DataPreparationTransformation.process(userActivityDF)
    val snapshotDF = SnapshotTransformation.process(preparedDF)

    snapshotDF.printSchema()

      snapshotDF
      .repartition(col("brandid"))
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy( "brandid", "daydate").parquet("results/data_warehouse/registereduser")
  }

  test("Step by step") {

    val userActivityDF = sparkSession
      .read.option("header", "true")
      .options(Map("inferSchema"->"true","delimiter"->"|"))
      .csv("seeds/samples/dwh/useractivity.csv")
      .withColumn(UsersColumns.daydate, col(UsersColumns.daydate).cast(StringType))

    val preparedDF = DataPreparationTransformation.process(userActivityDF).cache()
    preparedDF
      .where(col(UsersColumns.userid).equalTo("u1"))
      .show()
    val finalDF = SnapshotTransformation.process(preparedDF)
    finalDF
      .where(col(UsersColumns.userid).equalTo("u1"))
      .show(500)



    //val resultDF = enrichmentPostalCodeColumns(null,postalcodeColumnName,selectedColumns)


    //resultDF.show(10)


  }
}
