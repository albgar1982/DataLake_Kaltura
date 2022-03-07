package dataLake.layers.enriched_raw.kaltura.entitlements

import dataLake.core.layers.ComputationInterface
import dataLake.layers.enriched_raw.kaltura.entitlements.transformations.FilterAndEnrichInputEntitlementsTransformation
import org.apache.spark.sql.DataFrame

object entitlementsComputation extends ComputationInterface {

  override def process(originDataFrames: Map[String, DataFrame]): DataFrame = {

    val rawentitlementsDF = originDataFrames.get("entitlements").get


    val filteredAndEnrichedDF = FilterAndEnrichInputEntitlementsTransformation.process(rawentitlementsDF)

    filteredAndEnrichedDF
  }

}
