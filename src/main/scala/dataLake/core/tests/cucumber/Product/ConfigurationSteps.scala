package dataLake.core.tests.cucumber.Product

import dataLake.core.Spark
import dataLake.core.tests.cucumber.Utilities.RunVars
import io.cucumber.scala.{EN, ScalaDsl}

import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}

class ConfigurationSteps extends ScalaDsl with EN {

  Given("""the {string} with the Data Lake {string}, {string} and {string} config is to be evaluated""") { (tableName: String, environment: String, brandId: String, flavour: String) =>
    RunVars.currentTableName = tableName
  }

  When("""we read the data""") {

    RunVars.currentDF = Spark.configureSparkSession()
      .read
      .parquet(s"results/data_warehouse/${RunVars.currentTableName}")
    //.withColumn("prueba 2", lit("NA"))
    //.withColumn("otra_prEs", lit(null))
    //.withColumn("HOLA", lit("NA"))

    RunVars.currentDF.show(100,false)
    RunVars.currentDF.printSchema()

  }

  //var tableConfiguration: =
  /*
    Before {}
    Given("""the {string} with the Data Lake {string}, {string} and {string} config is to be evaluated""") { (tableName: String, environment: String, brandId: String, flavour: String) =>
      RunVars.currentTableName = tableName
    }
   */
}