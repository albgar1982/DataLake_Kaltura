package com.jumptvs.etls.enriched_raw.filmin.medias

import com.jumptvs.etls.config.{ArgumentsConfig, GlobalConfiguration}
import com.jumptvs.etls.db.DateRange
import com.jumptvs.etls.enriched_raw.EnrichedRaw
import com.jumptvs.etls.writer.{LocalWriter, S3Writer}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.net.URI

object MediasProcess extends EnrichedRaw with LazyLogging {

  override val enrichedRawTableName: String = "medias"
  val basePath: String = "enriched_raw/filmin/"

  private val uri: String => URI = path => new URI(path)
  private val naColumns = Seq(
  )

  override def run(arguments: ArgumentsConfig, dates: DateRange, config: GlobalConfiguration)(implicit sparkSession: SparkSession): Unit = {

    println(s"[EnrichedRaw][${config.environment.get}] ${enrichedRawTableName} Process")
    println(s"Start: ${dates.start} End: ${dates.end} ${arguments.executorMode}")

    val isFull = arguments.executorMode.equals("full")

    sparkSession.sparkContext.setJobGroup("Raw", "Read data")
    val (mediaSpainDF) = readSourcesTuple(dates,config, isFull)

    mediaSpainDF.show()


    //val unionMarketsDF = unionDataframes(mediaSpainDF, mediaPTDF)

    val computedEnrichedRawDF = computeEnrichedRaw(
      mediaSpainDF,
      config
    )

    logger.info(s"[EnrichedRaw][${config.environment.get}] ${enrichedRawTableName} Writing")
    sparkSession.sparkContext.setJobGroup(s"[EnrichedRaw] ${enrichedRawTableName}", "Writing data")

    uri(config.target.get.path).getScheme match {
      case "s3a" => S3Writer.write(
        isFull,
        config,
        computedEnrichedRawDF,
        s"${basePath}${enrichedRawTableName}",
        columns = naColumns
      )
      case _ => LocalWriter.write(
        isFull,
        config,
        computedEnrichedRawDF,
        s"${basePath}${enrichedRawTableName}",
        columns = naColumns
      )
    }
  }

  def unionDataframes(mediaSpainDF: DataFrame, mediaPTDF: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
    val spainCleanAndEnrichedDF = mediaSpainDF
      .withColumn("internalmarketsource", lit("spain")).select("id", "product_id", "brandid")

    val portugalCleanAndEnrichedDF = mediaSpainDF
      .withColumn("internalmarketsource", lit("portugal")).select("id", "product_id", "brandid")


    spainCleanAndEnrichedDF.union(portugalCleanAndEnrichedDF)

  }
  def computeEnrichedRaw(mediasDF: DataFrame, config: GlobalConfiguration)(implicit sparkSession: SparkSession): DataFrame = {

    mediasDF
      .withColumn("saludo", lit("adios"))
      .withColumn("daydate", lit("20210101"))
  }

  override def readSources(dates: DateRange, config: GlobalConfiguration, isFull: Boolean)(implicit session: SparkSession): Seq[DataFrame] = {
    null
  }

  def readSourcesTuple(dates: DateRange, config: GlobalConfiguration, isFull: Boolean)(implicit session: SparkSession):(DataFrame)= {
    (
      createProcessedSource("medias", dates, isFull, config, Map("delimiter" -> "|"),  readerCondition = "filmin_*/")
    )
  }
}