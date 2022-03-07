package dataLake.layers.enriched_raw.kaltura.catalogue

import dataLake.core.layers.ComputationInterface
import dataLake.layers.enriched_raw.kaltura.catalogue.transformations.FilterAndEnrichInputDataTransformation
import org.apache.spark.sql.DataFrame

object catalogueComputation extends ComputationInterface {

  override def process(originDataFrames: Map[String, DataFrame]): DataFrame = {

    val catalogDF = originDataFrames.get("catalogue").get

    val filteredAndEnrichedDF = FilterAndEnrichInputDataTransformation.process(catalogDF)
    filteredAndEnrichedDF

  }
}
