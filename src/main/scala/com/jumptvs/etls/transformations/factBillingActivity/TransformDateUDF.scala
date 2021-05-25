package com.jumptvs.etls.transformations.factBillingActivity

import java.text.SimpleDateFormat
import java.time.ZoneId
import java.time.format.DateTimeFormatter

import com.jumptvs.etls.transformations.BaseTransformation
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object TransformDateUDF extends BaseTransformation {

  override val transformationName: String = "getDateUDF"

  private def parseDate(date: String): SimpleDateFormat = {
    date match {
      case f1 if date.matches("^\\d{4}\\-(0[1-9]|1[012])\\-(0[1-9]|[12][0-9]|3[01])$") => new SimpleDateFormat("yyyy-MM-dd")
      case f2 if date.matches("[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1]) (2[0-3]|[01][0-9]):[0-5][0-9]:[0-5][0-9]") => new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      case f3 if date.matches("\\d{1,2}/\\d{1,2}/\\d{4} [0-9]{1,2}:[0-9]{1,2}") => new SimpleDateFormat("dd/MM/yyyy hh:mm")
      case _ => new SimpleDateFormat("yyyyMMdd")
    }
  }

  def getDate(dateStr: String, format: String): String = {
    val trimmedDate = dateStr.replace("\"", "").trim
    val dateTry = parseDate(trimmedDate).parse(trimmedDate)
    val localDate = dateTry.toInstant.atZone(ZoneId.systemDefault()).toLocalDateTime
    localDate.format(DateTimeFormatter.ofPattern(format))
  }

  override val transformationUDF: UserDefinedFunction = udf((date: String, format: String) => getDate(date, format))

}
