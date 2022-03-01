package com.jumpdd.dataLake.layers.enriched_raw.cms

import com.jumpdd.dataLake.core.layers.ComputationInterface
import com.jumpdd.dataLake.layers.cms.transformations.FilterAndEnrichInputDataTransformation
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.functions.{lit, _}
import org.apache.spark.sql.functions.when
//import org.apache.spark.sql.functions.regexp_replace


//config("spark.sql.debug.maxToStringFields",1000)


object cmsComputation extends ComputationInterface {

  override def process(originDataFrames: Map[String, DataFrame]): DataFrame = {

    val cmsDF = originDataFrames.get("cms").get

    val filteredAndEnrichedDF = FilterAndEnrichInputDataTransformation.process(cmsDF)


   // TODO review whether is necessary
    /*

    val concateDF = filteredAndEnrichedDF
      .withColumn("internalfulltitle",concat(col("internalserietitle"),lit(" S-"),col("internalseason"),lit("_E-"),
       col("internalepisode"),lit(' ')
        ,col("title")))
      .withColumn("internalnewgenres",when(col("internalgenres")==="Music"||col("internalgenres")==="music","music" ))

    concateDF.na.drop(Seq("internalgenres")).show()
    //concateDF

    val contenttypeDF = durationDF
      .na.fill("null",Array("internalepisode"))
      .withColumn("internalcontenttype",when(col("internalepisode")==="null", "movie").otherwise("episode"))
     */

    filteredAndEnrichedDF
  }
}
