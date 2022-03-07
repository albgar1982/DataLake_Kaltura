package dataLake.layers.data_warehouse.catalogue

import dataLake.core.layers.ComputationInterface
import dataLake.layers.data_warehouse.catalogue.transformations.ChangeNameAndAddColumnsTransformations
import org.apache.spark.sql.DataFrame

object catalogueComputation extends ComputationInterface {

  override def process(originDataFrames: Map[String, DataFrame]): DataFrame = {

    val catalogueDF = originDataFrames("catalogue")
    val factCatalogueDF = ChangeNameAndAddColumnsTransformations.process(catalogueDF)
    factCatalogueDF
  }
}
