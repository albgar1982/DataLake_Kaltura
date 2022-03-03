package dataLake.core.tests.cucumber.Product

import dataLake.core.{KnowledgeDataComputing, Spark}
import dataLake.core.configurations.ConfigurationEngine.getConfigurations
import dataLake.core.tests.cucumber.Utilities.RunVars
import io.cucumber.scala.{EN, ScalaDsl, Scenario}

class SparkSteps extends ScalaDsl with EN {


  Given("""setup spark""") {
    Spark.configureSparkSession(false)
    RunVars.sparkSession = KnowledgeDataComputing.sparkSession
  }

  When("""we read the data with spark""") {


    RunVars.currentDF = RunVars.sparkSession
      .read
      .parquet(s"results/data_warehouse/${RunVars.currentTableName}")
  }

  Then("""we obtain the metadata with spark""") {

    val sparkSchema = RunVars.currentDF.schema
    RunVars.originSchema = sparkSchema.fields.map(field => {
      (field.name, field.dataType.typeName)
    }).toMap

    val config = getConfigurations("DataWarehouse")

    RunVars.productSchema = config(RunVars.currentTableName)("schema")("product").obj
    RunVars.adHocSchema = config(RunVars.currentTableName)("schema")("adhoc").obj
  }
}

