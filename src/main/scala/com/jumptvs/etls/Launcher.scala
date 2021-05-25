package com.jumptvs.etls

import com.jumptvs.etls.config.{ArgumentsConfig, GlobalConfiguration}
import com.jumptvs.etls.environment.SparkEnvironment
import com.typesafe.scalalogging.LazyLogging

object Launcher extends LazyLogging {

  def main(args: Array[String]): Unit = {

    println("Running")
    logger.info("Running")

    val arguments = ArgumentsConfig.parse(args)

    if (arguments.isLeft) {
      println(arguments.left.get.getMessage)
    }

    val configuration = GlobalConfiguration.getConfig(arguments.right.get.configuration)
    val sparkSession = SparkEnvironment.createSparkSession(configuration)

    sparkSession.sparkContext.setLogLevel(configuration.logLevel.get)

    Runner.run(arguments.right.get, configuration)(SparkEnvironment.createSparkSession(configuration))

    println("Finished running")
    logger.info("Finished running")
  }
}
