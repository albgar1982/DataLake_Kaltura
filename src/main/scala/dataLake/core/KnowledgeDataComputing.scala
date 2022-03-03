package dataLake.core

import org.apache.spark.sql.SparkSession

import java.time.LocalDate
import java.time.format.DateTimeFormatter

case class DateRange(start: LocalDate, end: LocalDate)

class ExecutionControl(val start: LocalDate, val end: LocalDate) {
  case class Range(start: String, end: String)

   def daydateRanges(): Range = {
     Range(
       start.format(DateTimeFormatter.ofPattern("yyyyMMdd")),
       end.format(DateTimeFormatter.ofPattern("yyyyMMdd")),
     )
   }
}

object KnowledgeDataComputing {
  var executionControl: ExecutionControl = _
  var isFull: Boolean = _
  /** Stores the number of months to be retrieved when a full data set is to be computed. */
  var maxMonthsToCompute: Int = _
  /** f */
  var sparkSession: SparkSession = _
  var brandId: String = _
  var mapping: ujson.Value.Value = _
}
