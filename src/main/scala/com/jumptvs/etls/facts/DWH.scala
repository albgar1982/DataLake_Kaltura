package com.jumptvs.etls.facts

import com.jumptvs.etls.config.{ArgumentsConfig, GlobalConfiguration}
import com.jumptvs.etls.db.DateRange
import com.jumptvs.etls.environment.SparkEnvironment
import com.jumptvs.etls.processes.dwh.{CatalogueProcess, FactCatalogueProcess, PlaybacksProcess}
import org.apache.spark.sql.{DataFrame, SparkSession}

trait DWH {

  val factTableName: String

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
      case "playbacks" => new PlaybacksProcess().reader(dates, isFull, globalConfiguration, readerCondition = readerCondition)
      case "catalogue" => new CatalogueProcess().reader(dates, isFull, globalConfiguration, readerCondition = readerCondition)
      case "factCatalogue" => new FactCatalogueProcess().reader(dates, isFull, globalConfiguration, readerCondition = readerCondition)
    }
  }

}
