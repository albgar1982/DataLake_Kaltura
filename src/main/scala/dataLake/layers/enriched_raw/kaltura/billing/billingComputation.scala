package dataLake.layers.enriched_raw.kaltura.billing

import dataLake.core.layers.ComputationInterface
import dataLake.layers.enriched_raw.kaltura.billing.transformations.FilterAndEnrichInputBillingTransformation
import org.apache.spark.sql.DataFrame

object billingComputation extends ComputationInterface {

  override def process(originDataFrames: Map[String, DataFrame]): DataFrame = {

    val rawentitlementsDF = originDataFrames.get("transactions").get


    val filteredAndEnrichedDF = FilterAndEnrichInputBillingTransformation.process(rawentitlementsDF)

    filteredAndEnrichedDF
  }

}
