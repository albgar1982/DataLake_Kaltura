package com.jumptvs.etls.transformations

import cats.effect.IO
import com.jumptvs.etls.config.GlobalConfiguration
import com.jumptvs.etls.db.{DB, DateRange, Info}
import com.jumptvs.etls.model.m7.M7
import com.jumptvs.etls.utils.DataFrameUtils
import doobie.util.transactor.Transactor
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Forex {

  private def getOneQuotePerDay(dataSet: DataFrame): DataFrame = {
    val window = Window.partitionBy(M7.pairCurrency, M7.daydateCurrency).orderBy(col(M7.idCurrency).desc)
    dataSet
      .withColumn("row", row_number.over(window))
      .where(col("row").equalTo(1))
      .drop("row")
  }

  private def getCurrencies(df: DataFrame, config: GlobalConfiguration, dates: DateRange)(implicit sparkSession: SparkSession) = {
    config.isFull match {
      case true => getOneQuotePerDay(df)
        .filter(col(M7.pairCurrency).isin(M7.quotes: _*))
      case false => DataFrameUtils
        .filterColumnDates(getOneQuotePerDay(df), Seq(M7.daydateCurrency), dates)
        .filter(col(M7.pairCurrency).isin(M7.quotes: _*))
    }
  }

  def transform(config: GlobalConfiguration, dates: DateRange)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    val xaForex: Transactor[IO] = DB.getConnectionForex(config)
    val forexList = Info.readInfoFromTableForex(xaForex)

    getCurrencies(sparkSession.sparkContext.parallelize(forexList)
      .toDF(M7.idCurrency, M7.pairCurrency, M7.daydateCurrency, M7.quoteCurrency), config, dates)
  }

}
