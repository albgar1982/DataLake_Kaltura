package com.jumptvs.etls.utils

import java.time.LocalDate

import com.jumptvs.etls.db.DateRange

import cats.syntax.either._

object DatesProcessor {

  private def getDayteRange(startDate: LocalDate, endDay: LocalDate): DateRange = {
    DateRange(startDate, endDay)
  }

  def apply(startDate: LocalDate, endDay: LocalDate): Either[Throwable, DateRange] = {
    Either.catchNonFatal(getDayteRange(startDate,endDay))
  }
}