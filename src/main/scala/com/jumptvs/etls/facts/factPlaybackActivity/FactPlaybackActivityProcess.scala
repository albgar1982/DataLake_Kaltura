package com.jumptvs.etls.facts.factPlaybackActivity

import com.jumptvs.etls.config.{ArgumentsConfig, GlobalConfiguration}
import com.jumptvs.etls.db.DateRange
import com.jumptvs.etls.facts.DWH
import com.jumptvs.etls.model.Global
import com.jumptvs.etls.model.m7.Facts
import com.jumptvs.etls.remover.Remover
import com.jumptvs.etls.tables.PlaybacksTable
import com.jumptvs.etls.utils.DataFrameUtils
import com.jumptvs.etls.writer.{LocalWriter, S3Writer}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.net.URI

object FactPlaybackActivityProcess extends DWH with LazyLogging {

  override val factTableName: String = "fact_playbackactivity"
  val basePath: String = "datawarehouse/"

  private val uri: String => URI = path => new URI(path)
  private val naColumns = Seq(
    Global.genresColName,
    Global.subscriberidColName,
    Global.subscriptionidColName,
    Global.networkColName,
    Global.contentidColName,
    Global.seasonColName,
    Global.fulltitleColName,
    Global.originColName,
    Global.titleColName,
    Global.producttypeColName,
    Global.regioncodeColName
  )

  override def run(arguments: ArgumentsConfig, dates: DateRange, config: GlobalConfiguration)(implicit sparkSession: SparkSession): Unit = {

    logger.info(s"[${config.environment.get}] ${factTableName} Process")
    println(s"[${config.environment.get}] ${factTableName} Process")
    println(s"Start: ${dates.start} End: ${dates.end} ${arguments.executorMode}")

    val isFull = arguments.executorMode.equals("full")

    val configColumnsMap: Map[String, Any] = Map(
      Global.brandidColName -> config.brandId.getOrElse("Missing Brand ID!!"),
      Global.customeridColName -> config.customerIdentifier.getOrElse("Missing Customer ID")
    )

    sparkSession.sparkContext.setJobGroup(factTableName, "Read data")
    val (playbacksDF, factCatalogueDF) = readSourcesTuple(dates, config, isFull)

    val preMappingDF = createPreMappingsDataFrame(
      playbacksDF,
      factCatalogueDF,
      config
    )

    val finalDF = createFinalDataFrame(preMappingDF, configColumnsMap)

    logger.info(s"[JUMP][${config.environment.get}] ${factTableName} Writing")

    sparkSession.sparkContext.setJobGroup(factTableName, "Writing data")

    Remover.remove(
      sparkSession,
      s"${config.target.get.path}${basePath}${factTableName}/brandid=${config.brandId.get}",
      dates,
      isFull
    )

    uri(config.target.get.path).getScheme match {
      case "s3a" => S3Writer.write(isFull,
        config,
        finalDF.select(PlaybacksTable.finalColumns.map(col): _*),
        s"${basePath}${factTableName}",
        columns = naColumns
      )
      case _ => LocalWriter.write(
        isFull,
        config,
        finalDF.select(PlaybacksTable.finalColumns.map(col): _*),
        s"${basePath}${factTableName}",
        columns = naColumns
      )
    }
  }

  def createPreMappingsDataFrame(playbacksDF: DataFrame, catalogueDF: DataFrame, config: GlobalConfiguration)(implicit sparkSession: SparkSession): DataFrame = {

    val mostRecentContentWindow = Window.partitionBy("contentid").orderBy(col("daydate").desc)
    
    val mostRecentContentsDF = catalogueDF
      .withColumn("mostrecentcontent", dense_rank().over(mostRecentContentWindow))
      .where(col("mostrecentcontent").equalTo(1))
      .drop("daydate")

    val cleanedPlaybacksDF = playbacksDF.drop("brandid")

    cleanedPlaybacksDF
      .join(mostRecentContentsDF, playbacksDF.col("contentid").equalTo(mostRecentContentsDF.col("contentid")))
      .drop(mostRecentContentsDF.col("contentid"))
  }

  def createFinalDataFrame(originDF: DataFrame, configColumnsMap: Map[String, Any])(implicit sparkSession: SparkSession): DataFrame = {

    val enrichedDF = originDF
      .withColumn(Global.deviceColName, lit(0))
      .withColumn(Global.devicetypeColName, lit(0))
      .withColumn(Global.monthdateColName, concat(substring(col("internalseconddate"), 1, 6), lit("01")))
      .withColumn(Global.yeardateColName, concat(substring(col("internalseconddate"), 1, 4), lit("01"), lit("01")))
      .withColumn(Global.secondDateColName, col("internalseconddate"))
      .withColumn(Global.hourdateColName, concat(substring(col("internalseconddate"), 1, 10)))

    val insertedJumpConfigDF = DataFrameUtils.withColumns(configColumnsMap, enrichedDF, lit)
    val literalColumnsDF = DataFrameUtils.withColumns(Facts.addNewPlaybacksColumns, insertedJumpConfigDF, lit)
    DataFrameUtils.renameColumns(Facts.renamedPlaybackActivityColumns, literalColumnsDF)
  }

  override def readSources(dates: DateRange, config: GlobalConfiguration, isFull: Boolean)(implicit session: SparkSession): Seq[DataFrame] = {
    null
  }

  def readSourcesTuple(dates: DateRange, config: GlobalConfiguration, isFull: Boolean)(implicit session: SparkSession):(DataFrame, DataFrame)= {
    (
      createProcessedSource("playbacks", dates, isFull, config, readerCondition = factTableName),
      createProcessedSource("factCatalogue", dates, isFull, config, readerCondition = factTableName)
    )
  }

}
