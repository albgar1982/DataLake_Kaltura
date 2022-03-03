package dataLake.core.spark

import dataLake.core.KnowledgeDataComputing.sparkSession
import dataLake.core.{KnowledgeDataComputing, Spark}
import org.apache.spark.sql.DataFrame

import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDate}

object DataFrameFactory {

  def betweenDates(start: LocalDate, end: LocalDate, columnName: String = "daydate"): DataFrame = {
    val currentSpark = sparkSession
    import currentSpark.implicits._

    val daysBetween = Duration
      .between(
        start.atStartOfDay(),
        end.atStartOfDay()
      ).toDays

    val rowsForDateDF = Seq.newBuilder[String]
    val dtFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

    for (currentDay <- 0L to daysBetween) {
      val dateToString = start.plusDays(currentDay).format(dtFormatter)
      rowsForDateDF += dateToString
    }

    rowsForDateDF.result().toDF()
      .withColumnRenamed("value", columnName)
  }

}