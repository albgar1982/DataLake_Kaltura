package dataLake.core

import dataLake.core.configurations.ConfigurationEngine
import dataLake.core.kapi.Configurations
import dataLake.core.utilities.Logger

object Launcher extends Logger {

  def main(args: Array[String]): Unit = {

    val argumentsConfig = ArgumentsConfig.parse(args)

    if (argumentsConfig.isLeft) {
      error(s"Error reading parameters: ${argumentsConfig.left.get.getMessage}")
      throw new Exception("")
    }

    val arguments = argumentsConfig.right.get

    KnowledgeDataComputing.sparkSession = Spark.configureSparkSession(arguments.remote.toBoolean)

    Configurations.getConfigs(
      arguments.brandId, arguments.flavour, arguments.branch, arguments.environment,
      arguments.kapiConfigUrl,
      arguments.kapiConfigToken
    )

    //Coordinator.run(arguments, dataLakeConfiguration, brandConfiguration)
  }
}
