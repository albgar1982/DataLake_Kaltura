package Cucumber.Product

import Cucumber.Utilities.RunVars
import com.jumpdd.core.knowledge.configurations.ConfigurationEngine.getConfigurations
import io.cucumber.scala.{EN, ScalaDsl}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, count, lit, when}
import org.apache.spark.sql.types.StructType
import org.junit.Assert._

import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}

class ConfigurationSteps extends ScalaDsl with EN {

  Given("""the {string} with the Data Lake {string}, {string} and {string} config is to be evaluated""") { (tableName: String, environment: String, brandId: String, flavour: String) =>
    RunVars.currentTableName = tableName
  }

  When("""we read the data""") {

    RunVars.currentDF = RunVars.sparkSession
      .read
      .parquet(s"results/data_warehouse/${RunVars.currentTableName}")
    //.withColumn("prueba 2", lit("NA"))
    //.withColumn("otra_prEs", lit(null))
    //.withColumn("HOLA", lit("NA"))

    //RunVars.currentDF.show(100,false)

  }

  //var tableConfiguration: =
/*
  Before {}

  Given("""the {string} with the Data Lake {string}, {string} and {string} config is to be evaluated""") { (tableName: String, environment: String, brandId: String, flavour: String) =>
    RunVars.currentTableName = tableName
  }

 */
}