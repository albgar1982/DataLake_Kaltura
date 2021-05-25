package com.jumptvs.etls.db

import cats.effect.{ContextShift, IO}
import com.jumptvs.etls.config.GlobalConfiguration
import doobie.util.ExecutionContexts
import doobie.util.transactor.Transactor
import org.apache.spark.sql.SparkSession

object DB {

  private def getProperty(property: String, config: GlobalConfiguration)(implicit sparkSession: SparkSession): String =
    sparkSession.sparkContext.getConf.get(property)

  def getConnection(configuration: GlobalConfiguration)(implicit sparkSession: SparkSession): Transactor[IO] = {
    DB.getTransactor(getProperty("spark.service.user.postgresql.host", configuration),
       getProperty("spark.service.user.postgresql.port", configuration),
       getProperty("spark.service.user.postgresql.user", configuration),
       getProperty("spark.service.user.postgresql.pass", configuration),
       getProperty("spark.service.user.postgresql.database", configuration))
  }

  def getConnectionPostalCodes(configuration: GlobalConfiguration)(implicit sparkSession: SparkSession): Transactor[IO] = {
    DB.getTransactor(getProperty("spark.service.user.postgresql.host", configuration),
      getProperty("spark.service.user.postgresql.port", configuration),
      getProperty("spark.service.user.postgresql.user", configuration),
      getProperty("spark.service.user.postgresql.pass", configuration),
      getProperty("spark.service.user.postgresql.postalCodeDatabases", configuration))
  }

  def getConnectionForex(configuration: GlobalConfiguration)(implicit sparkSession: SparkSession): Transactor[IO] = {
    DB.getTransactor(getProperty("spark.service.user.postgresql.host", configuration),
      getProperty("spark.service.user.postgresql.port", configuration),
      getProperty("spark.service.user.postgresql.user", configuration),
      getProperty("spark.service.user.postgresql.pass", configuration),
      getProperty("spark.service.user.postgresql.forexDatabase", configuration))
  }

  def getTransactor(host: String,
                    port: String,
                    user: String,
                    pass: String,
                    database: String): Transactor[IO] = {
    implicit val context: ContextShift[IO] = IO.contextShift(ExecutionContexts.synchronous)
    Transactor.fromDriverManager[IO](
      driver = "org.postgresql.Driver",
      url = s"jdbc:postgresql://$host:$port/$database",
      user,
      pass,
      ExecutionContexts.synchronous
    )
  }
}
