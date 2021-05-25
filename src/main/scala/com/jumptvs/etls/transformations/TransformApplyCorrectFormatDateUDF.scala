package com.jumptvs.etls.transformations

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import com.jumptvs.etls.utils.DateUtils

object TransformApplyCorrectFormatDateUDF extends BaseTransformation {

  override val transformationName: String = "getCorrectDateFormatUDF"

  def parseFormat(date: String, finalFormat: String): String = {
    DateUtils.convertStringToString(date, finalFormat) match {
      case Left(e) => e.getMessage
      case Right(dateString) => dateString
    }
  }

  override val transformationUDF: String => UserDefinedFunction = finalFormat => udf((date: String) =>
    parseFormat(date, finalFormat)
  )
}
