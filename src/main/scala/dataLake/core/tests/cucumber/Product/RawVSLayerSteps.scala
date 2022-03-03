package dataLake.core.tests.cucumber.Product

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count}

import scala.util.control.Breaks.breakable

object RawVSLayerSteps {

  def checkComparisionTables(originDF: DataFrame, sampleDF: DataFrame): String = {
    val resultDF = sampleDF.join(originDF, sampleDF.columns.toSeq, "leftanti")

    resultDF.collect().mkString("\n")
      .replaceAll("\\[", "| ")
      .replaceAll("]", " |")
      .replaceAll(",", " | ")
  }

  def checkComparisonOfQuantitiesBetweenTables(originDF: DataFrame, sampleDF: DataFrame, dateColumnName: String): String = {

    val originQuantityColumn: String = "originquantity"
    val originDateColumn: String = s"origin$dateColumnName"

    var result: String = ""

    val agregateDF = originDF
      .groupBy(col(dateColumnName).alias(originDateColumn))
      .agg(count(col("*")).alias(originQuantityColumn))

    val resultDF = agregateDF
      .join(
        sampleDF,
        sampleDF.col(dateColumnName).equalTo(agregateDF.col(originDateColumn)),
        "fullouter"
      )

    resultDF.collect().foreach(row => {
      val originDateValue = row.get(0)
      val originQuantityValue = row.get(1)
      val dateValue = row.get(2)
      val quantityValue = row.get(3)

      breakable {

        if (originDateValue != null && dateValue != null && originQuantityValue != quantityValue) {
          result += s"Quantities do not match for the date $dateValue ~> original table $originQuantityValue != sample table $quantityValue\n"
          breakable()
        }

        if (originDateValue == null) {
          result += s"For the date $dateValue in the sample table there is no crossover against the original table\n"
          breakable()
        }

        if (dateValue == null) {
          result += s"For the date $originDateValue in the original table there is no crossover against the sample table\n"
        }
      }
    })

    result
  }
}
