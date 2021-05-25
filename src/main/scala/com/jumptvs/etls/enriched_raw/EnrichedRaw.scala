package com.jumptvs.etls.enriched_raw

import com.jumptvs.etls.config.{ArgumentsConfig, GlobalConfiguration}
import com.jumptvs.etls.db.DateRange
import com.jumptvs.etls.environment.SparkEnvironment
import com.jumptvs.etls.processes.enriched_raw.{CatalogueProcess, MediasSpainProcess}
import org.apache.spark.sql.{DataFrame, SparkSession}

trait EnrichedRaw {

  val enrichedRawTableName: String

  def run(arguments: ArgumentsConfig, dates: DateRange,
          config: GlobalConfiguration)
         (implicit sparkSession: SparkSession): Unit

  def readSources(dates: DateRange, config: GlobalConfiguration, isFull: Boolean)(implicit session: SparkSession = SparkEnvironment.createSparkSession(config)): Seq[DataFrame]

  def createProcessedSource(pr: String,
                            dates: DateRange,
                            isFull: Boolean,
                            globalConfiguration: GlobalConfiguration,
                            readerOptions: Map[String, String] = Map(),
                            readerCondition: String = null
                           )(implicit sparkSession: SparkSession): DataFrame = {
    pr match {
      case "medias" => new MediasSpainProcess().reader(dates, isFull, globalConfiguration, readerOptions, readerCondition = readerCondition)
      case "catalogue" => new CatalogueProcess().reader(dates, isFull, globalConfiguration, readerCondition = readerCondition)
    }
  }

}
