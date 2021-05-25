package com.jumptvs.etls.facts.factCatalogue

import com.jumptvs.etls.config.{ArgumentsConfig, GlobalConfiguration}
import com.jumptvs.etls.db.DateRange
import com.jumptvs.etls.facts.DWH
import com.jumptvs.etls.model.Global
import com.jumptvs.etls.remover.Remover
import com.jumptvs.etls.tables.CatalogueTable
import com.jumptvs.etls.writer.{LocalWriter, S3Writer}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import java.net.URI

object FactCatalogueProcess extends DWH with LazyLogging {

  override val factTableName: String = "fact_catalogue"
  val basePath: String = "datawarehouse/"

  private val uri: String => URI = path => new URI(path)
  private val snaColumns = Seq(
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
    val (catalogueDF) = readSourcesTuple(dates,config, isFull)

    val preMappingDF = createPreMappingsDataFrame(
      catalogueDF,
      config
    )

    val finalDF = createFinalDataFrame(preMappingDF, configColumnsMap)

    logger.info(s"[${config.environment.get}] ${factTableName} Writing")

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
        finalDF.select(CatalogueTable.finalColumns.map(col): _*),
        s"${basePath}${factTableName}",
        columns = snaColumns
      )
      case _ => LocalWriter.write(
        isFull,
        config,
        finalDF.select(CatalogueTable.finalColumns.map(col): _*),
        s"${basePath}${factTableName}",
        columns = snaColumns
      )
    }
  }

  def createPreMappingsDataFrame(catalogueDF: DataFrame, config: GlobalConfiguration)(implicit sparkSession: SparkSession): DataFrame = {

    catalogueDF
      .withColumn("customerid", lit("0ef7c99a-355f-11eb-9916-2b51b3ca4c38"))
      .withColumn("contentid", col("internalcontentid"))
      .withColumn(Global.contentdurationColName, col("internalduration"))
      .withColumn("title", col("duration"))
      .withColumn("contenttype", col("subtype"))
      .withColumn("season", col("seasonnumber"))
      .withColumn("episode", col("episodenumber"))
      .withColumn("groupcontenttype", lit("SVOD"))
      .withColumn("network", lit("Hallmark"))
      .withColumn("genres", col("internalgenres"))
      .withColumn(Global.channelColName, Global.naColumn)
      .withColumn("countrycode", lit("NA"))
      .withColumn("serietitle", when(col("contenttype").equalTo("TV_SHOW_EPISODE"), col("tvshowname")).otherwise(lit("NA")))
      .withColumn("title", when(col("contenttype").equalTo("TV_SHOW_EPISODE"), col("episodename")).otherwise(col("tvshowname")))
      .withColumn("fulltitle",
        when(col("contenttype").equalTo("TV_SHOW_EPISODE"),  functions.concat(col("serietitle"), lit(" S-"), col("seasonnumber"), lit(" E-"), col("episodenumber"), lit(" "), col("episodename")))
          .otherwise(col("tvshowname"))
      )
  }

  def createFinalDataFrame(df: DataFrame, configColumnsMap: Map[String, Any])(implicit sparkSession: SparkSession): DataFrame = {
    df
  }

  override def readSources(dates: DateRange, config: GlobalConfiguration, isFull: Boolean)(implicit session: SparkSession): Seq[DataFrame] = {
    null
  }

  def readSourcesTuple(dates: DateRange, config: GlobalConfiguration, isFull: Boolean)(implicit session: SparkSession):(DataFrame)= {
    (
      createProcessedSource("catalogue", dates, isFull, config, readerCondition = factTableName)
    )
  }

}
