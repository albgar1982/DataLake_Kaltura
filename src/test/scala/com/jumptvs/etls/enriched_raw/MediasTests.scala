package com.jumptvs.etls.enriched_raw

import com.jumptvs.etls.config.{ArgumentsConfig, GlobalConfiguration}
import com.jumptvs.etls.db.DateRange
import com.jumptvs.etls.enriched_raw.filmin.medias.MediasProcess
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.StreamMetadata.format
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.{BeforeAndAfterAll, EitherValues, FlatSpecLike, Matchers}

import java.time.LocalDate

class MediasTests extends FlatSpecLike with Matchers with EitherValues with BeforeAndAfterAll {
  implicit val spark: SparkSession = SparkSession.builder().appName("CatalogueTests").master("local[*]")
    .getOrCreate()

  val args = Array(
    "--brandId", "1d24fe6e-02c0-11e7-ba09-6b9a99cdcfd7",
    "--configuration", "full",
    "--executorMode", "full",
    "--coordinators", "enrichedRaw",
    "--tables", "medias"
  )

  override def beforeAll(): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    super.beforeAll()

  }

  it should "Execution" in {



    val source = scala.io.Source.fromURL(getClass.getResource(s"/configs/local.json"))
    val configuration = parse(source.mkString).extract[GlobalConfiguration]
    val parameters = ArgumentsConfig.parse(args)

    val arguments = parameters.right.get


    val date = DateRange(LocalDate.of(2020, 11, 1), LocalDate.of(2020, 12, 8))
    MediasProcess
      .run(arguments, date, configuration)(spark)
  }

  it should "Process by steps results" in {
    val source = scala.io.Source.fromURL(getClass.getResource(s"/configs/local.json"))
    val configuration = parse(source.mkString).extract[GlobalConfiguration]
    val parameters = ArgumentsConfig.parse(args)

    val arguments = parameters.right.get

    val mediasDF = spark.read
      .option("multiline", "true")
      .option("delimiter", "|")
      .option("header", "true")
      .csv("./seeds/raw/filmin/medias/")

    val finalDF = MediasProcess.computeEnrichedRaw(mediasDF, configuration)
    finalDF.show()
  }

  it should "Transformations" in {

    val tableMedias = spark.read
      .option("multiline", "true")
      .option("delimiter", "|")
      .option("header", "true")
      .option("basePath", "./seeds/raw/*")
      .csv("./seeds/raw/*/medias/")

    tableMedias
      .show(5000)
  }
}