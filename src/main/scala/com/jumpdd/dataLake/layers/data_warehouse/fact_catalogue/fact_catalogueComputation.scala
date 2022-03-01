package com.jumpdd.dataLake.layers.data_warehouse.fact_catalogue

import com.jumpdd.dataLake.KnowledgeDataComputing
import com.jumpdd.dataLake.{DateRange, ExecutionControl}
import com.jumpdd.dataLake.core.layers.{ComputationInterface, PlaybacksColumns}
import com.jumpdd.dataLake.layers.cms.cmsColumns
import com.jumpdd.dataLake.layers.data_warehouse.fact_catalogue.fact_catalogueModel.tmpEndDate
import com.jumpdd.dataLake.layers.data_warehouse.fact_catalogue.transformations.{CleanFactCatalogueTransformation, CreateFactCatalogueColumnsTransformation, DragContentsBetweenDatesTransformation, PrepareCMSDataTransformation}
import org.apache.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{max, regexp_replace, _}




object fact_catalogueComputation extends ComputationInterface {


  override def process(originDataFrames: Map[String, DataFrame]): DataFrame = {

    val cmsDF = originDataFrames("cms")
    val executionControl = KnowledgeDataComputing.executionControl


    cmsDF.show()
    cmsDF.printSchema()
    cmsDF
  }
}