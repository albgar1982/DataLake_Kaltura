package com.jumptvs.etls

import com.jumptvs.etls.config.{ArgumentsConfig, GlobalConfiguration}
import com.jumptvs.etls.coordinators.{DWHCoordinator, EnrichedRawCoordinator}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

object Runner extends LazyLogging {

  def run(argumentsConfiguration: ArgumentsConfig,
          globalConfiguration: GlobalConfiguration)(implicit sparkSession: SparkSession): Unit = {

    logger.info(s"[JUMP][${globalConfiguration.environment.get}] Configuration received ${argumentsConfiguration.toString}")

    globalConfiguration.isFull = argumentsConfiguration.executorMode.equals("full")


    if (argumentsConfiguration.coordinators.contains("enrichedRaw")) {
      enrichedRAWCoordinator(argumentsConfiguration, globalConfiguration)
    }

    if (argumentsConfiguration.coordinators.contains("datawarehouse")) {
      dwhCoordinator(argumentsConfiguration, globalConfiguration)
    }

    logger.info(s"[${globalConfiguration.environment.get}] Runner end")
  }

  private def dwhCoordinator(argumentsConfiguration: ArgumentsConfig,
                             globalConfiguration: GlobalConfiguration)(implicit sparkSession: SparkSession): Unit = {

    new DWHCoordinator(argumentsConfiguration, globalConfiguration)
        .withProcesses
        .run()
  }

  private def enrichedRAWCoordinator(argumentsConfiguration: ArgumentsConfig,
                             globalConfiguration: GlobalConfiguration)(implicit sparkSession: SparkSession): Unit = {

    new EnrichedRawCoordinator(argumentsConfiguration, globalConfiguration)
        .withProcesses
        .run()
  }

}