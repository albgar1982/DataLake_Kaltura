package dataLake.layers.data_warehouse.billing

import dataLake.core.KnowledgeDataComputing
import dataLake.core.layers.ComputationInterface
import dataLake.layers.data_warehouse.billing.transformations.ChangeNameAndAddColumnsTransformations
import org.apache.spark.sql.DataFrame


object billingComputation extends ComputationInterface {

  override def process(originDataFrames: Map[String, DataFrame]): DataFrame = {

    val billingDF = originDataFrames("fact_catalogue")

    val executionControl = KnowledgeDataComputing.executionControl


    val factBillingDF = ChangeNameAndAddColumnsTransformations.process(billingDF)

    factBillingDF


  }

}
