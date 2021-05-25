package com.jumptvs.etls.utils

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

import cats.syntax.either._

object DateUtils {

  private val formatter: String => DateTimeFormatter = format => DateTimeFormatter.ofPattern(format)

  def convertStringToLocalDate(date: String, format: String): LocalDate = {
    LocalDate.parse(date, formatter(format))
  }

  def convertStringToLocalDateTime(date: String, format: String): LocalDateTime = {
    LocalDateTime.parse(date, formatter(format))
  }

  def convertLocalDateToString(date: LocalDate, format: String): String = {
    date.format(formatter(format))
  }

  def convertLocalDateTimeToString(date: LocalDateTime, format: String): String = {
    date.format(formatter(format))
  }

  def convertLocalDateStringToLocalDateTime(date: String, format: String): LocalDateTime = {
    LocalDate.parse(date, formatter(format)).atStartOfDay
  }

  def addDaysToDate(date: String, days: Int, format: String): LocalDate = {
    convertStringToLocalDate(date, format).plusDays(days)
  }

  def addDaysToDateTime(date: String, days: Int, format: String): LocalDateTime = {
    convertStringToLocalDateTime(date, format).plusDays(days)
  }

  def removeDaysToDate(date: String, days: Int, format: String): LocalDate = {
    convertStringToLocalDate(date, format).minusDays(days)
  }

  def convertStringToString(date: String, format: String, finalFormat: String, finalType: String = "localdate"): String = {
    finalType match {
      case "localdate" => convertLocalDateToString(convertStringToLocalDate(date, format), finalFormat)
      case "localdatetime" => convertLocalDateTimeToString(convertStringToLocalDateTime(date, format), finalFormat)
      case "locadatetolocaldatetime" => convertLocalDateTimeToString(convertLocalDateStringToLocalDateTime(date, format), finalFormat)
      case _ => throw new RuntimeException("Unknown Type")
    }
  }

  def convertStringToString(date: String, finalFormat: String): Either[Throwable, String] = {
    Either.catchNonFatal(convertLocalDateTimeToString(LocalDateTime.parse(date, parseDate(date)), finalFormat))
  }

  def convertStringToStringForLocalDateTime(date: String, finalFormat: String): Either[Throwable, String] = {
    Either.catchNonFatal(convertLocalDateTimeToString(LocalDateTime.parse(date, parseDate(date)), finalFormat))
  }

  def convertStringToStringForLocalDate(date: String, finalFormat: String): Either[Throwable, String] = {
    Either.catchNonFatal(convertLocalDateToString(LocalDate.parse(date, parseDate(date)), finalFormat))
  }

  private def parseDate(date: String): DateTimeFormatter = {
    date match {
      case f1 if date.matches("^(\\d{4}-\\d{2}-\\d{2})$") => formatter("yyyy-MM-dd")
      case f2 if date.matches("^(\\d{4}-\\d{2}-\\d{2} (2[0-3]|[01][0-9]):[0-5][0-9]:[0-5][0-9])$") => formatter("yyyy-MM-dd HH:mm:ss")
      case f3 if date.matches( "^(\\d{2}/\\d{2}/\\d{4})$") => formatter("dd/MM/yyyy")
      case f4 if date.matches( "^(\\d{2}/\\d{2}/\\d{4} (2[0-3]|[01][0-9]):[0-5][0-9]:[0-5][0-9])$") => formatter("dd/MM/yyyy HH:mm:ss")
      case f5 if date.matches( "^(\\d{1}-\\d{2}-\\d{4} ([0-9]):[0-5][0-9]:[0-5][0-9])$") => formatter("d-MM-yyyy H:mm:ss")
      case f6 if date.matches( "^(\\d{1}-\\d{2}-\\d{4})$") => formatter("d-MM-yyyy")
      case f7 if date.matches( "^(\\d{2}-\\d{2}-\\d{4})$") => formatter("dd-MM-yyyy")
      case f8 if date.matches( "^(\\d{1}-\\d{2}-\\d{4} (2[0-3]|[01][0-9]):[0-5][0-9]:[0-5][0-9])$") => formatter("d-MM-yyyy HH:mm:ss")
      case f9 if date.matches( "^(\\d{2}-\\d{2}-\\d{4} (2[0-3]|[01][0-9]):[0-5][0-9]:[0-5][0-9])$") => formatter("dd-MM-yyyy HH:mm:ss")
      case f10 if date.matches( "^(\\d{2}-\\d{2}-\\d{4} ([0-9]):[0-5][0-9]:[0-5][0-9])$") => formatter("dd-MM-yyyy H:mm:ss")
      case _ => formatter("yyyyMMdd")
    }
  }

}
