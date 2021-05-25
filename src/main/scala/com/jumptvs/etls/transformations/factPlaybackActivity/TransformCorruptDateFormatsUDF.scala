package com.jumptvs.etls.transformations.factPlaybackActivity

import java.time.LocalDate

import com.jumptvs.etls.model.Dates
import com.jumptvs.etls.model.m7.M7
import com.jumptvs.etls.transformations.BaseTransformation
import com.jumptvs.etls.utils.DateUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object TransformCorruptDateFormatsUDF extends BaseTransformation {

  override val transformationName: String = "getCorrectDateUDF"

  private def processDate(dateStr: String, daydate: String): String = {
    if (dateStr.startsWith("0")) daydate.substring(0, 4) + dateStr.substring(4)
    else dateStr
  }

  private def getDate(date: Option[String], daydate: String): String = {
    date match {
      case Some(dateStr) => processDate(dateStr, daydate)
      case _ => DateUtils.convertStringToString(daydate, Dates.yyyyMMdd, M7.dateFormat, "locadatetolocaldatetime")
    }
  }

  override val transformationUDF: UserDefinedFunction = udf((date: String, daydate: String) =>
    getDate(Option(date), daydate)
  )

}
