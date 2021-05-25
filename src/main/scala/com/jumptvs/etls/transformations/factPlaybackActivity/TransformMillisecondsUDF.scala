package com.jumptvs.etls.transformations.factPlaybackActivity

import com.jumptvs.etls.transformations.BaseTransformation
import com.jumptvs.etls.utils.{isLargerThanZero, isDigit}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object TransformMillisecondsUDF extends BaseTransformation {

  override val transformationName: String = "transformMilliseconds"

  private def set24HLimit(value: Int): Int = {
    if (value > 86400) 86400000
    else value*1000
  }

  private def multiply(value: Int): Int = {
    value match {
      case isLargerThanZero() => set24HLimit(value)
      case _ => 0
    }
  }

  def transformTime(time: Option[String]): Int = {
    time match {
      case Some(value) if isDigit(value) => multiply(value.toInt)
      case _ => 0
    }
  }

  override val transformationUDF: UserDefinedFunction = udf((time: String) =>
    transformTime(Option(time)))

}
