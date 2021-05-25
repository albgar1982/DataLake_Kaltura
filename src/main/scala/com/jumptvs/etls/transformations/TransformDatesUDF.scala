package com.jumptvs.etls.transformations

import java.text.SimpleDateFormat
import java.time.ZoneId
import java.time.format.DateTimeFormatter

import com.jumptvs.etls.model.{Dates, Global}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

object TransformDatesUDF extends BaseTransformation {

  override val transformationName: String = "splitDatesUDF"

  private val emptySplittedDate = Seq(SplittedDate(
    Global.naColName,
    Global.naColName,
    Global.naColName,
    Global.naColName,
    Global.naColName,
    Global.naColName,
    Global.naColName,
    Global.naColName,
    Global.naColName))

  val dateStructType: ArrayType = ArrayType(new StructType()
    .add(Dates.secondate, StringType)
    .add(Dates.hourdate, StringType)
    .add(Dates.daydate, StringType)
    .add(Dates.monthdate, StringType)
    .add(Dates.yearDate, StringType)
    .add(Dates.yearMonth, StringType)
    .add(Dates.day, StringType)
    .add(Dates.month, StringType)
    .add(Dates.year, StringType)
  )

  def checkDate(date: String): Seq[SplittedDate] = {
    Option(date) match {
      case Some(dateString) => splitDates(dateString)
      case _ => emptySplittedDate
    }
  }

  def splitDates(dateStr: String): Seq[SplittedDate] = {
    val trimmedDate = dateStr.replace("\"", "").trim
    if (trimmedDate.isEmpty) emptySplittedDate
    else {
      val dateTry = parseDate(trimmedDate).parse(trimmedDate)
      val localDate = dateTry.toInstant.atZone(ZoneId.systemDefault()).toLocalDateTime
      Seq(SplittedDate(localDate.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
        .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
        localDate.format(DateTimeFormatter.ofPattern(Dates.yyyyMMddHH)),
        localDate.format(DateTimeFormatter.ofPattern(Dates.yyyyMMdd)),
        localDate.format(DateTimeFormatter.ofPattern(Dates.yyyyMM01)),
        localDate.format(DateTimeFormatter.ofPattern(Dates.yyyy)),
        localDate.format(DateTimeFormatter.ofPattern(Dates.yyyyMM)),
        localDate.getDayOfMonth.toString,
        localDate.getMonthValue.toString,
        localDate.getYear.toString))
    }
  }

  override val transformationUDF: UserDefinedFunction = udf(checkDate _, dateStructType)

  private def parseDate(date: String): SimpleDateFormat = {
      date match {
        case f1 if date.matches("^\\d{4}\\-(0[1-9]|1[012])\\-(0[1-9]|[12][0-9]|3[01])$") => new SimpleDateFormat("yyyy-MM-dd")
        case f2 if date.matches("[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1]) (2[0-3]|[01][0-9]):[0-5][0-9]:[0-5][0-9]") => new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
        case _ => new SimpleDateFormat("yyyyMMdd")
      }
  }
}

case class SplittedDate(secondDate: String,
                        hourDate: String,
                        dayDate: String,
                        monthDate: String,
                        yearDate: String,
                        yearMonth: String,
                        day: String,
                        month: String,
                        year: String)



