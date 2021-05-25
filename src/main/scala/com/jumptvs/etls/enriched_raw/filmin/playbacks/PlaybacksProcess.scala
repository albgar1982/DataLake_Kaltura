package com.jumptvs.etls.enriched_raw.filmin.playbacks

import com.jumptvs.etls.config.{ArgumentsConfig, GlobalConfiguration}
import com.jumptvs.etls.db.DateRange
import com.jumptvs.etls.enriched_raw.EnrichedRaw
import com.jumptvs.etls.model.{Dates, Global}
import com.jumptvs.etls.remover.Remover
import com.jumptvs.etls.utils.DatesProcessor
import com.jumptvs.etls.writer.{LocalWriter, S3Writer}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.net.URI
import java.time.format.DateTimeFormatter

object PlaybacksProcess extends EnrichedRaw with LazyLogging {

  override val enrichedRawTableName: String = "playbacks"
  val basePath: String = "enriched_raw/accedo_one/"

  private val uri: String => URI = path => new URI(path)
  private val naColumns = Seq(
  )

  override def run(arguments: ArgumentsConfig, dates: DateRange, config: GlobalConfiguration)(implicit sparkSession: SparkSession): Unit = {

    logger.info(s"[EnrichedRaw][${config.environment.get}] ${enrichedRawTableName} Process")
    println(s"[EnrichedRaw][${config.environment.get}] ${enrichedRawTableName} Process")
    println(s"Start: ${dates.start} End: ${dates.end} ${arguments.executorMode}")

    val isFull = arguments.executorMode.equals("full")

    sparkSession.sparkContext.setJobGroup("Raw", "Read data")
    val rawPlaybacksDF = readSourcesTuple(dates, config, isFull)

    var computedEnrichedRawDF = computeEnrichedRaw(
      rawPlaybacksDF,
      config
    )

    logger.info(s"[EnrichedRaw][${config.environment.get}] ${enrichedRawTableName} Writing")
    sparkSession.sparkContext.setJobGroup(s"[EnrichedRaw] ${enrichedRawTableName}", "Writing data")

    var customDates = dates

    if (!isFull) {
      customDates = DatesProcessor(dates.start.plusDays(1), dates.end).right.get
      computedEnrichedRawDF = computedEnrichedRawDF.where(col(Global.daydateColName).gt(customDates.start.format(DateTimeFormatter.ofPattern(Dates.yyyyMMdd))))
    }

    Remover.remove(
      sparkSession,
      s"${config.target.get.path}${basePath}${enrichedRawTableName}/brandid=${config.brandId.get}",
      customDates,
      isFull
    )

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

  def computeEnrichedRaw(rawPlaybacksDF: DataFrame, config: GlobalConfiguration)(implicit sparkSession: SparkSession): DataFrame = {

    val topWindow = Window.partitionBy("userid", "contentid").orderBy(col("playbackstartdatetime").asc)

    rawPlaybacksDF
      .drop("subscriptiontype", "usertype", "deviceip")
      .withColumn("date", to_timestamp(col("playbackstartdatetime"), "yyyy-MM-dd'T'HH:mm:ss"))
      .withColumn("previousevent", lag("date", 1).over(topWindow))
      .withColumn("diffseconds", (unix_timestamp(col("date")) - unix_timestamp(col("previousevent"))).cast(DataTypes.IntegerType))
      .withColumn("firstplayback", col("diffseconds").gt(15).or(col("diffseconds").isNull))
      .withColumn("sameplaybacksession", col("diffseconds").between(0, 15))
      .withColumn("playbacksession", when(col("firstplayback").equalTo(true), md5(concat(col("contentid"), col("playbackstartdatetime"), col("userid")))))
      .where(col("firstplayback").equalTo(true))
      .select(
        col("brandid"),
        date_format(col("date"), "yyyyMMdd").alias("daydate"),
        col("userid"),
        col("playbackstartdatetime"),
        col("contentid"),
        date_format(col("date"), "yyyyMMddHHmmss").alias("internalseconddate"),
        lit(0).alias("internalplaybacktime")
      )
  }

  override def readSources(dates: DateRange, config: GlobalConfiguration, isFull: Boolean)(implicit session: SparkSession): Seq[DataFrame] = {
    null
  }

  def readSourcesTuple(dates: DateRange, config: GlobalConfiguration, isFull: Boolean)(implicit session: SparkSession): (DataFrame) = {
    (
      createProcessedSource("playbacks", dates, isFull, config, readerCondition = enrichedRawTableName)
    )
  }
}
