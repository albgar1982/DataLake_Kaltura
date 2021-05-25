package com.jumptvs.etls.transformations.factCatalogue

import com.jumptvs.etls.model.Global
import com.jumptvs.etls.transformations.BaseTransformation
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object TransformSeasonUDF extends BaseTransformation {

  override val transformationName: String = "getSeasonUDF"

  private def getSeason(season: String, seriesTitle: Option[String]): String = {
    seriesTitle match {
      case Some(seriesTitleName) => season
      case _ => Global.naColName
    }
  }

  override val transformationUDF: UserDefinedFunction = udf((season: String, seriesTitle: String) => {
    getSeason(season, Option(seriesTitle))
  })

}
