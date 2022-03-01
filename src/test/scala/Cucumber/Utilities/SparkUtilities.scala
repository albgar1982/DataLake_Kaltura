package Cucumber.Utilities

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkUtilities {

  def setup(): Unit = {
    import org.apache.log4j.{Level, Logger}

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

    val sparkParameters = Map[String, String](
      "spark.sql.legacy.allowUntypedScalaUDF" -> "true"
    )

    val appConf = new SparkConf()
      .setAppName("Data writer to Elasticsearch")
      .setAll(sparkParameters)
      .setMaster("local[*]")

    RunVars.sparkSession = SparkSession.builder().config(appConf).getOrCreate()
    RunVars.sparkSession.sparkContext.setLogLevel("ERROR")
  }
}
