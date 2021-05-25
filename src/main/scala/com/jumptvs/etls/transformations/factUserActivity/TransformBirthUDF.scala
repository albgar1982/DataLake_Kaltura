package com.jumptvs.etls.transformations.factUserActivity

import com.jumptvs.etls.model.{Dates, Global}
import com.jumptvs.etls.transformations.BaseTransformation
import com.jumptvs.etls.utils.DateUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf


object TransformBirthUDF extends BaseTransformation {

  override val transformationName: String = "getBirthUDF"

  private def getDate(date: String): String = {
    DateUtils.convertStringToStringForLocalDate(date.trim, Dates.yyyyMMdd) match {
      case Left(e) => e.getMessage
      case Right(date) => date
    }
  }

  def getBirthDate(birth: Option[String]): String = {
    birth match {
      case Some(date) => date
      case _ => Global.naColName
    }
  }

  override val transformationUDF: UserDefinedFunction = udf((birth: String) =>
    getBirthDate(Option(birth)))
}
