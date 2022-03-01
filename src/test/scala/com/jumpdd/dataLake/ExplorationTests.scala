package com.jumpdd.dataLake

import Cucumber.Utilities.RunVars
import com.jumpdd.dataLake.core.ArgumentsConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class ExplorationTests extends AnyFlatSpec with should.Matchers {
  "Check" should "playbacks" in {
    import org.apache.log4j.{Level, Logger}

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

    val sparkParameters = Map[String, String](
      "spark.sql.legacy.allowUntypedScalaUDF" -> "true"
    )

    val appConf = new SparkConf()
      .setAppName("Data writer to Elasticsearch")
      .setAll(sparkParameters)
      .setMaster("local[*]")

    val spark = SparkSession.builder().config(appConf).getOrCreate()

    // Transform json to parquet
    val playDF = spark.read.json("seeds/smx_viewer_level.json")
    playDF.write.parquet("seeds/raw/brightcove/playbacks/playbacks.parquet")

    // Transform csv to parquet
    // val users = spark.read.parquet("seeds/raw/beacon/users")
   //   .show()

    //val entitlements = spark.read.options(Map("delimiter"->",","header"->"true"))
      //.csv("seeds/seed_entitlements.csv")
      //.show()
      //entitlements.write.parquet("/seeds/raw/beacon/entitlements/entitlements.parquet")

    //spark.read.parquet("results/data_warehouse/fact_useractivity"

  }
}
