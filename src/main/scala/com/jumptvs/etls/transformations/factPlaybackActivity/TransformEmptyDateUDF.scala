package com.jumptvs.etls.transformations.factPlaybackActivity

import java.time.LocalDate

import com.jumptvs.etls.transformations.BaseTransformation
import com.jumptvs.etls.transformations.factPlaybackActivity.TransformContentTypesUDF.getContentType
import com.jumptvs.etls.utils.DateUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object TransformEmptyDateUDF extends BaseTransformation {

  override val transformationName: String = "getDateTypeUDF"

  private def getDate(date: Option[String]): String = {
    date match {
      case Some(dateStr) => dateStr
      case _ => DateUtils.convertLocalDateToString(LocalDate.now, "yyyyMMdd")
    }
  }

  override val transformationUDF: UserDefinedFunction = udf((date: String) =>
    getDate(Option(date))
  )

}
