package com.jumptvs.etls.utils

import com.jumptvs.etls.db.DateRange
import com.jumptvs.etls.model.{Dates, Global}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

import scala.collection.Map

object DataFrameUtils {

  def unionDataFramesDifferentColumns(leftDf: DataFrame,
                                      rightDf: DataFrame): DataFrame = {
    val total = leftDf.columns.toSet.union(rightDf.columns.toSet).toList.distinct
    leftDf.select(expr(leftDf.columns.distinct.toSet, total.toSet): _*)
      .union(rightDf.select(expr(rightDf.columns.toSet, total.toSet): _*))
  }

  private def expr(myCols: Set[String], allCols: Set[String]): List[Column] = {
    allCols.toList.map(x => x match {
      case x if myCols.contains(x) => col(x)
      case _ => lit(null).as(x)
    })
  }

  def getMaxDayDate(df: DataFrame, column: String): String = {
    import df.sparkSession.implicits._
    df.select(column).distinct().agg(max(col(column)).as(column)).as[String].head()
  }

  def filterColumnDates(df: DataFrame, columnDates: Seq[String], dates: DateRange): DataFrame = {
    df.filter(columnDates.map(name =>
      col(name).cast(StringType) > lit(DateUtils.convertLocalDateToString(dates.start, Dates.yyyyMMdd)) &&
      col(name) <= lit(DateUtils.convertLocalDateToString(dates.end, Dates.yyyyMMdd)))
      .reduce(_ or _))
  }

  def filterColumnDates(df: DataFrame, startDate: String, endDate: String, dates: DateRange): DataFrame = {
    df.filter((col(startDate) > lit(DateUtils.convertLocalDateToString(dates.start, Dates.yyyyMMdd)) &&
      col(startDate) <= lit(DateUtils.convertLocalDateToString(dates.end, Dates.yyyyMMdd))
      ) ||
      (col(endDate) > lit(DateUtils.convertLocalDateToString(dates.start, Dates.yyyyMMdd)) &&
        col(endDate) <= lit(DateUtils.convertLocalDateToString(dates.end, Dates.yyyyMMdd))))
  }

  def filterColumn(df: DataFrame, columnDates: Seq[String], value: String): DataFrame = {
    df.filter(columnDates.map(name => col(name) === value)
      .reduce(_ or _))
  }

  def getSelectedColumns(df: DataFrame, columns: Seq[String]): DataFrame = {
    df.select(columns.map(col): _*)
  }

  def unionByNameCommonElements(df1: DataFrame, df2: DataFrame): DataFrame = {
    val commonElements = df1.columns.intersect(df2.columns).map(col)
    df1.select(commonElements: _*) unionByName df2.select(commonElements: _*)
  }

  def withColumns(literalsMap: Map[String, Any], df: DataFrame, func: String => Column): DataFrame =
    literalsMap.foldLeft(df) {
      case(df, (key,value)) =>
        value match {
          case (valueKey: String, variableType: DataType) => df.withColumn(key, func(valueKey).cast(variableType))
          case value: String => df.withColumn(key, func(value))
          case _ => df.withColumn(key, lit(value))
        }
    }

  def renameColumns(columnsMap: Map[String, String], df: DataFrame): DataFrame = {
    dropColumns(columnsMap.values.toSet.toSeq, withColumns(columnsMap, df, col))
  }

  def dropColumns(columns: Seq[String], df: DataFrame): DataFrame = {
    df.drop(columns: _*)
  }

  def selectColumns(columns: Seq[String], df: DataFrame): DataFrame = {
    df.select(columns.map(col): _*)
  }

  def filterIfEqual(df: DataFrame, column: String, conditions: Seq[Any]): DataFrame = {
    df.filter(conditions.map(condition => col(column) === condition).reduce(_ || _))
  }

  def filterIfNotEqual(df: DataFrame, column: String, conditions: Seq[String]): DataFrame = {
    df.filter(conditions.map(condition => col(column).notEqual(condition)).reduce(_ && _))
  }

  def orderColumnsByDate(df: DataFrame, columns: Seq[String], date: String, format: String, desc: Boolean=true): DataFrame = {
    import org.apache.spark.sql.expressions._

    val partition = desc match {
      case true => Window.partitionBy(columns.map(col): _*).orderBy(to_date(col(date), format).as(date).desc)
      case false => Window.partitionBy(columns.map(col): _*).orderBy(to_date(col(date), format).as(date))
    }

    df.withColumn("row", row_number.over(partition))
      .where(col("row") === 1).drop("row")

  }

  def cleanAndExplode(df: DataFrame, eventName: String): DataFrame = {
    val explodedDF = df.withColumn(eventName,
      explode(col(s"${eventName}EventJump")))
      .drop(s"${eventName}EventJump")
    explodedDF.select(explodedDF.columns.distinct.map(name => if (name.equals(eventName)) {
      col(s"$eventName.*")
    } else col(name)): _*)
  }

  def getColumns(df: DataFrame, extraColumn: String = null, extraColumns: Seq[String] = null): Seq[String] = {
    if (extraColumn != null) df.columns :+ extraColumn
    else if (extraColumns != null) df.columns ++ extraColumns
    else df.columns
  }

  def cleanVoidColumn(df: DataFrame, nameCol: String): DataFrame = {
    df.withColumn(nameCol, when(df(nameCol) === "", lit(Global.naColName)).otherwise(col(nameCol)))
  }

  def createEmptyDataFrame(columns: Seq[String])(implicit sparkSession: SparkSession): DataFrame = {
    val schema = StructType(columns.map(fieldName => StructField(fieldName, StringType, true)))
    sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema)
  }

}