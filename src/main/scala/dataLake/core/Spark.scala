package dataLake.core

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark {

  def configureSparkSession(isRemote: Boolean = false): SparkSession = {

    import org.apache.log4j.{Level, Logger}

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

    val appConf: SparkConf = new SparkConf()

    val sparkParameters = Map[String, String](
      "spark.sql.legacy.allowUntypedScalaUDF" -> "true",
      "spark.driver.bindAddress" -> "127.0.0.1",
      "spark.sql.analyzer.failAmbiguousSelfJoin" -> "true"
    )

    isRemote match {
      case true => SparkSession.builder.config(appConf).config("spark.sql.legacy.allowUntypedScalaUDF", "true").appName("Data Lake").getOrCreate()
      case false => SparkSession.builder.config(appConf)
        .config("spark.sql.legacy.allowUntypedScalaUDF", "true").config("spark.driver.bindAddress", "127.0.0.1").config("spark.sql.analyzer.failAmbiguousSelfJoin", "false")
        .master("local[*]").appName("Data Lake").getOrCreate()
    }
  }
}
