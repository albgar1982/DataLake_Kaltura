package com.jumptvs.etls

import com.jumptvs.etls.model.Dates
import com.jumptvs.etls.utils.DatesProcessor
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, rand}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.scalatest.{BeforeAndAfterAll, EitherValues, FlatSpecLike, Matchers}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class IntegrationTests extends FlatSpecLike with Matchers with EitherValues with BeforeAndAfterAll {
  implicit val spark: SparkSession = SparkSession.builder().appName("CatalogueTests").master("local[*]")
    .getOrCreate()

  override def beforeAll(): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    super.beforeAll()
  }

  it should "Test Dates" in {
    val dates = DatesProcessor(LocalDate.now().minusDays(3), LocalDate.now()).right.get
    println(dates.start.format(DateTimeFormatter.ofPattern(Dates.yyyyMMdd)))
  }

  it should "Read fake Playbacks" in {

    val factCatalogueDF = spark
      .read
      .parquet("./results/datawarehouse/fact_playbackactivity")
      .groupBy("contenttype").agg(functions.count("*"))
      .show(false)
  }

  it should "Read Catalogue" in {

    val factCatalogueDF = spark
      .read
      .parquet("./results/datawarehouse/fact_catalogue")
      .show(false)
  }

  it should "Read Fake Recommenations" in {
    val similarItemsDF = spark
      .read
      .parquet("./results/ai/es/similar_items")
      .where(col("similarContentId").equalTo("ITEM#PN388552"))
      .show(false)
  }


  it should "Compute Fake Recommenations" in {

    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(s"./results/ai/es"), true)

    val factCatalogueDF = spark
      .read
      .parquet("./results/datawarehouse/fact_catalogue")
      .where(col("season").isin("NA", "1").and(col("episode").isin("NA", "1")))


    val seedPopularityDF = factCatalogueDF
      .orderBy(rand())
      .select(
        col("contentid").alias("contentId"),
        functions.monotonically_increasing_id().alias("rank"),
        lit("{}").alias("rawCatalogue"),
        col("brandid")
      )

    seedPopularityDF
      .repartition(col("brandid"))
      .write
      .partitionBy("brandid")
      .parquet("./results/ai/es/popularity")

    val seedRecommendationsDF = recommendations(factCatalogueDF, "5f3c3971-aff1-443c-9876-c48a2e8adb26")
      .union(recommendations(factCatalogueDF, "d18366bc-e0da-4634-bbae-8e698b0fa2cb"))

    seedRecommendationsDF
      .repartition(col("brandid"))
      .write
      .partitionBy("brandid")
      .parquet("./results/ai/es/recommendation")

    val seedSimilarItemDF = factCatalogueDF
      .orderBy(rand())
      .withColumn("similarityScore", rand())
      .withColumn("rank", rand())
      .select(
        col("contentid").alias("contentId"),
        functions.lag("contentid", 1, 0).over(Window.orderBy("similarityScore", "rank")).alias("similarContentId"),
        col("similarityScore"),
        col("rank"),
        lit("{}").alias("rawCatalogue"),
        col("brandid")
      ).where(col("similarContentId").notEqual("0"))

    seedSimilarItemDF
      .repartition(col("brandid"))
      .write
      .partitionBy("brandid")
      .parquet("./results/ai/es/similar_items")

    val popularityDF = spark
      .read
      .parquet("./src/test/resources/seeds/data lake/ai/elastic_output/popularity")

    val recommendationDF = spark
      .read
      .parquet("./src/test/resources/seeds/data lake/ai/elastic_output/recommendation")

    val similarItemsDF = spark
      .read
      .parquet("./src/test/resources/seeds/data lake/ai/elastic_output/similar_items")

  }

  def recommendations(catalogueDF: DataFrame, subscriberId: String, limit: Integer = 20): DataFrame = {
    catalogueDF
      .orderBy(rand())
      .limit(limit)
      .select(
        col("contentid").alias("contentId"),
        functions.monotonically_increasing_id().alias("rank"),
        lit(subscriberId).alias("subscriberId"),
        col("contenttype").alias("contentType"),
        col("genres"),
        lit("{}").alias("rawCatalogue"),
        col("brandid")
      )
  }


/*
  `customerid` string,
  `subscriberid` string,
  `subscriptionid` string,
  `genres` string,
  `network` string,
  `contentid` string,
  `title` string,
  `serietitle` string,
  `episode` string,
  `season` string,
  `fulltitle` string,
  `contenttype` string,
  `groupcontenttype` string,
  `hourdate` string,
  `monthdate` string,
  `yeardate` string,
  `year` string,
  `yearmonth` string,
  `day` string,
  `month` string,
  `type` string,
  `playbacktime` bigint,
  `contentduration` bigint,
  `bufferingtime` bigint,
  `bitrate` bigint,
  `origin` string,
  `countrycode` string,
  `regioncode` string,
  `device` int,
  `devicetype` int
 */
}