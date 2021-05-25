package com.jumptvs.etls.transformations.factPlaybackActivity

import com.jumptvs.etls.model.Global
import com.jumptvs.etls.transformations.BaseTransformation
import com.jumptvs.etls.utils.{isDigit, isLargerThanZero}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object TransformNumberUDF extends BaseTransformation {

  override val transformationName: String = "getNumberUDF"

  private def checkNumber(number: String): String = {
    number.toInt match {
      case greaterThanZeroOrEqual if number.toInt >= 0 => number
      case _ => "NA"
    }
  }

  private def getNumber(number: Option[String]): String = {
    number match {
      case Some(value) if isDigit(value) => checkNumber(value)
      case _ => "NA"
    }
  }

  override val transformationUDF: UserDefinedFunction = udf((number: String) => {
    getNumber(Option(number))
  })
}
