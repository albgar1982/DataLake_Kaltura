package com.jumptvs.etls.coordinators

import cats.effect.IO
import com.jumptvs.etls.config.{ArgumentsConfig, GlobalConfiguration, Processes}
import com.jumptvs.etls.db.{DB, DateRange}
import com.jumptvs.etls.facts.DWH
import com.jumptvs.etls.facts.factCatalogue.FactCatalogueProcess
import com.jumptvs.etls.facts.factPlaybackActivity.FactPlaybackActivityProcess
import com.jumptvs.etls.utils.{DataUtils, DatesProcessor}
import com.typesafe.scalalogging.LazyLogging
import doobie.util.transactor.Transactor
import org.apache.spark.sql.SparkSession

import java.time.LocalDate

class DWHCoordinator(argumentsConfiguration: ArgumentsConfig,
                     globalConfiguration: GlobalConfiguration,
                     process: Seq[Processes] = Seq())
                    (implicit sparkSession: SparkSession) extends LazyLogging {

  val processToExecute: Seq[Processes] = process
  implicit val xa: Transactor[IO] = DB.getConnection(globalConfiguration)

  def run(): Unit = {

    globalConfiguration.brandId = Some(argumentsConfiguration.brandId)

    println(s"===\n" +
      s"= DWH\n" +
      s"===\n"
    )

    val dates = argumentsConfiguration.executorMode match {
      case "full" => DatesProcessor(LocalDate.now().minusDays(1000), LocalDate.now()).right.get
      case "delta" => DatesProcessor(LocalDate.now().minusDays(3), LocalDate.now()).right.get
      case "range" =>
        DatesProcessor(
          LocalDate.parse(argumentsConfiguration.startDate),
          LocalDate.parse(argumentsConfiguration.endDate)
        ).right.get
      case _ => DatesProcessor(LocalDate.now().minusDays(3), LocalDate.now()).right.get
    }

    processToExecute
      .filter(process => isRunnable(process.table))
      .foreach(process => process.table match {
      case "fact_playbackactivity" =>
        FactPlaybackActivityProcess.run(argumentsConfiguration, dates, globalConfiguration)

        //processor(Info.getPlaybackInfo, Info.setPlaybackInfo, FactPlaybackActivityProcess, process.days, argumentsConfiguration.brandId)
      case "fact_catalogue" =>
        FactCatalogueProcess.run(argumentsConfiguration, dates, globalConfiguration)
        //processor(Info.getCatalogInfo, Info.setCatalogInfo, null, process.days, argumentsConfiguration.brandId)
      case _ => println(s"Unknown table : ${process.table}")
    })
  }

  def processor(getInfo: (ArgumentsConfig, GlobalConfiguration) => Either[Throwable, DateRange],
                setInfo: (String, LocalDate, LocalDate) => Either[Throwable, Int],
                runner: DWH,
                days: String,
                brandid: String): Unit = {

    val dates = DatesProcessor(LocalDate.now().minusDays(10000), LocalDate.now()).right.get
    runner.run(argumentsConfiguration, dates, globalConfiguration)(sparkSession)

    /*
    processDates(getInfo, days) match {
      case Right(dates) => {
        println(s"dates.start: ${dates.start} dates.end: ${dates.end}")
        runner.run(argumentsConfiguration, dates, globalConfiguration)(sparkSession)
        if (!argumentsConfiguration.executorMode.equals("range")) {
          setInfo(brandid, dates.start, dates.end)
        }
      }
      case Left(exception) => logger.info(s"[JUMP][${globalConfiguration.environment.get}] Failing ${exception}")
    }

     */
  }

  def isRunnable(table: String): Boolean = {
    if (argumentsConfiguration.tables.isEmpty) true
    else if (DataUtils.createSeqFromUnderscoreSeparatedString(argumentsConfiguration.tables).contains(table)) true
    else false
  }

  def processDates(infoProcessor: (ArgumentsConfig, GlobalConfiguration) => Either[Throwable, DateRange], days: String): Either[Throwable, DateRange] = {
    argumentsConfiguration.executorMode match {
      case "full" => DatesProcessor(LocalDate.now().minusDays(1), LocalDate.now().minusDays(1))
      case "delta" => DatesProcessor(LocalDate.now().minusDays(days.toInt), LocalDate.now().minusDays(1))
      case "range" =>
        DatesProcessor(
          LocalDate.parse(argumentsConfiguration.startDate),
          LocalDate.parse(argumentsConfiguration.endDate)
        )
      case _ => DatesProcessor(LocalDate.now().minusDays(days.toInt), LocalDate.now().minusDays(1))
    }
  }

  def withProcess(process: Processes): DWHCoordinator = {
    new DWHCoordinator(argumentsConfiguration, globalConfiguration, processToExecute :+ process)
  }

  def withProcesses: DWHCoordinator = {
    new DWHCoordinator(argumentsConfiguration, globalConfiguration, processToExecute ++ globalConfiguration.processes)
  }

}

