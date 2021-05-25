package com.jumptvs.etls.transformations.factCatalogue

import com.jumptvs.etls.model.Global
import com.jumptvs.etls.transformations.BaseTransformation
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object TransformContentTypeUDF extends BaseTransformation {

  override val transformationName: String = "getContentTypeUDF"

  private def getContentType(seriesTitle: Option[String]): String = {
    seriesTitle match {
      case Some(seriesTitleName) => "Series"
      case _ => Global.naColName
    }
  }

  override val transformationUDF: UserDefinedFunction = udf((seriesTitle: String) => {
    getContentType(Option(seriesTitle))
  })

}
