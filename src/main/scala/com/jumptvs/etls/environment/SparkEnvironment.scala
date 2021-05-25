package com.jumptvs.etls.environment

import com.jumptvs.etls.config.GlobalConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkEnvironment {

  val appConf: SparkConf = new SparkConf()
    //.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

  def createSparkSession(config: GlobalConfiguration): SparkSession = {
    config.environment.get match {
      case "remote" => SparkSession.builder.config(appConf).appName(appNameGenerator(config)).getOrCreate()
      case _ => SparkSession.builder.config(appConf).master("local[*]").getOrCreate()
    }
  }

  private def appNameGenerator(config: GlobalConfiguration) = s"Running in ${config.environment.get} for ${config.brandId}"
}
