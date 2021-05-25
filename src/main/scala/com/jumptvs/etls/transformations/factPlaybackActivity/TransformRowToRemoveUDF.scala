package com.jumptvs.etls.transformations.factPlaybackActivity

import com.jumptvs.etls.model.Tmp
import com.jumptvs.etls.model.m7.M7
import com.jumptvs.etls.transformations.BaseTransformation
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.IntegerType

object TransformRowToRemoveUDF extends BaseTransformation {

  override val transformationName: String = "getRowTypeUDF"

  private def checkRow(subscription: String, row: String, counter: String, subscriptionWinner: String): Boolean = {
    if (subscription == subscriptionWinner && counter.toInt == 1) false
    else if (row.toInt == 1) false
    else true
  }

  override val transformationUDF: UserDefinedFunction = udf((subscription: String, row: String, counter: String, subscriptionWinner: String) =>
    checkRow(subscription, row, counter, subscriptionWinner)
  )

  def transformationUDFWithUDF (subscription: Column, row: Column, counter: Column, subscriptionWinner: Column) :Boolean = {
    (subscription == subscriptionWinner && counter.cast(IntegerType) == 1) || (row.cast(IntegerType) == 1)

  }

}