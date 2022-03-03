package dataLake.core.layers

import dataLake.core.layers.schema.ColumnObject
import dataLake.core.utilities.Logger
import org.apache.spark.sql.DataFrame

trait ComputationInterface extends Logger {

  def process(originDataFrames: Map[String, DataFrame]): DataFrame

  def setDefaultValues(productSchema: Array[ColumnObject], adHocSchema: Array[ColumnObject] = Array(),  originDF: DataFrame): DataFrame = {
    val schema = productSchema ++ adHocSchema

    val result = schema
      .groupBy(_.name)
      .mapValues(_.map(_.defaultValue))

    null
  }
}
