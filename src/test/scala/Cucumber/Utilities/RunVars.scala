package Cucumber.Utilities

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.LinkedHashMap

object RunVars {

  var sparkSession: SparkSession = _
  var currentDF: DataFrame = _
  var currentDataLakeLayer: String = _
  var currentTableName: String = _

  var originSchema: Map[String, String] = _
  var productSchema: LinkedHashMap[String, ujson.Value.Value] = _
  var adHocSchema: LinkedHashMap[String, ujson.Value.Value] = _

}
