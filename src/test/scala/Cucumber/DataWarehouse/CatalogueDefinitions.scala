package Cucumber.dataWarehouse

import Cucumber.Utilities.BddSpark.DataFrameConvertibleOps
import Cucumber.Utilities.RunVars
import com.jumpdd.core.knowledge.configurations.ConfigurationEngine.getConfigurations
import io.cucumber.datatable.DataTable
import io.cucumber.scala.{EN, ScalaDsl}
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, count, lit, when}
import org.apache.spark.sql.types.StructType
import org.junit.Assert.fail
import org.slf4j.LoggerFactory

import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}

class CatalogueDefinitions extends ScalaDsl with EN {


  /*
  val sparkParameters = Map[String, String](
    "spark.sql.legacy.allowUntypedScalaUDF" -> "true"
  )
  val appConf = new SparkConf()
    .setAppName("Data writer to Elasticsearch")
    .setAll(sparkParameters)
    .setMaster("local[*]")

  val spark = SparkSession.builder().config(appConf).getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  Given("""^a data table with the "([^"]*)"$""") { (tableName: String, data: DataTable) =>
    val rawDF = data.toDF(spark)
    rawDF.createOrReplaceTempView(tableName)
    rawDF.show()
  }

  Given("""compute FactUserActivity from {string} raw""") { (tableName: String) =>
    val rawDF = spark.sqlContext.sql(s"select * from $tableName").toDF()
    val usersDF2 = rawDF.na.fill("")
  }

   */

}