package Cucumber.Product

import Cucumber.Utilities.{RunVars, SparkUtilities}
import com.jumpdd.core.knowledge.configurations.ConfigurationEngine.getConfigurations
import com.jumpdd.dataLake.core.Spark
import io.cucumber.scala.{EN, ScalaDsl, Scenario}
import org.apache.spark.sql.functions.lit

class SparkSteps extends ScalaDsl with EN {


  Given("""setup spark""") {
    SparkUtilities.setup()
  }

  When("""we read the data with spark""") {

    //parquet(s"results/data_warehouse/${RunVars.currentTableName}")

    RunVars.currentDF = RunVars.sparkSession
      .read
      .parquet(s"results/data_warehouse/${RunVars.currentTableName}")
      .withColumn("prueba 2", lit("NA"))
      .withColumn("otra_prEs", lit(null))
      .withColumn("HOLA", lit("NA"))
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

