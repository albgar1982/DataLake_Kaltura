package com.jumpdd.dataLake

import com.jumpdd.core.knowledge.configurations.ConfigurationEngine
import com.jumpdd.dataLake.core.{ArgumentsConfig, Spark}
import com.jumpdd.dataLake.core.utilities.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Launcher extends Logger {

  def main(args: Array[String]): Unit = {

    headMessage("Launcher")

    val argumentsConfig = ArgumentsConfig.parse(args)

    if (argumentsConfig.isLeft) {
      println(argumentsConfig.left.get.getMessage)
    }

    val arguments = argumentsConfig.right.get

    KnowledgeDataComputing.sparkSession = Spark.configureSparkSession()
    //val dataLakeConfiguration = ConfigurationEngine.getConfigurations(arguments.brandId, arguments.environment, arguments.flavour)
    val dataLakeConfiguration = ConfigurationEngine.getConfigurations("etl")

    Coordinator.run(arguments, dataLakeConfiguration)
  }
}
