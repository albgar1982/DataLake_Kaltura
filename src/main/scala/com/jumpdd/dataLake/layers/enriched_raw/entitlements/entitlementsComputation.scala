package com.jumpdd.dataLake.layers.enriched_raw.entitlements

import com.jumpdd.dataLake.core.layers.ComputationInterface
import org.apache.spark.sql.DataFrame

object entitlementsComputation extends ComputationInterface {

  override def process(originDataFrames: Map[String, DataFrame]): DataFrame = {

    val entitlementsDF = originDataFrames.get("entitlements").get
    val entitlementsEnrichedDF = EntitlementsFilterAndEnrichInputDataTransformation.process(entitlementsDF)

    entitlementsEnrichedDF


  }
}
