package dataLake.layers.enriched_raw.kaltura.households

import dataLake.core.layers.ComputationInterface
import dataLake.layers.enriched_raw.kaltura.entitlements.transformations.FilterAndEnrichInputEntitlementsTransformation
import org.apache.spark.sql.DataFrame

object householdsComputation extends ComputationInterface {

  override def process(originDataFrames: Map[String, DataFrame]): DataFrame = {

    val rawhouseholdsDF = originDataFrames.get("households").get

    val filteredAndEnrichedDF = FilterAndEnrichInputEntitlementsTransformation.process(rawhouseholdsDF)

    filteredAndEnrichedDF
  }

}
