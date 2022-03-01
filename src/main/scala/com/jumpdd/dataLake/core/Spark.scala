package com.jumpdd.dataLake.core

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark {

  var currentSparkSession: SparkSession = null

  def configureSparkSession(): SparkSession = {

    val appConf: SparkConf = new SparkConf()

    val sparkParameters = Map[String, String](
      "spark.sql.legacy.allowUntypedScalaUDF" -> "true"
    )

    currentSparkSession = SparkSession.builder
      .config(appConf)
      .config("spark.driver.bindAddress", "127.0.0.1") // TODO: Remove for remote. This workaround is only for Miguel
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true") // TODO: Remove for remote
      .master("local[*]") // TODO: Remote
      .appName("Data Lake")

      .getOrCreate()

    currentSparkSession.sparkContext.setLogLevel("WARN")
    currentSparkSession
  }
}
