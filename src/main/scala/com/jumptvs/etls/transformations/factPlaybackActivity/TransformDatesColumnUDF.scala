package com.jumptvs.etls.transformations.factPlaybackActivity

import com.jumptvs.etls.transformations.BaseTransformation
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object TransformDatesColumnUDF extends BaseTransformation {

  override val transformationName: String = "getDatesColumnTypeUDF"

  private def checkRange(start: Option[String], end: Option[String], date: Option[String]): Boolean = {
    (start, end, date) match {
      case(Some(startStr), Some(endStr), Some(dateStr)) => (dateStr.toInt <= endStr.toInt) && (dateStr.toInt >= startStr.toInt)
      case _ => false
    }
  }

  override val transformationUDF: UserDefinedFunction = udf((start: String, end: String, date: String) =>
    checkRange(Option(start), Option(end), Option(date))
  )

}
