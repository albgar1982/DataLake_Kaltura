package com.jumptvs.etls.transformations.factPlaybackActivity

import com.jumptvs.etls.model.Global
import com.jumptvs.etls.transformations.BaseTransformation
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object TransformSeasonUDF extends BaseTransformation {

  override val transformationName: String = "getSeasonUDF"

  private def checkSeason(season: Option[String]): String = {
    season match {
      case Some(seasonValue) => seasonValue
      case _ => Global.naColName
    }
  }

  private def getSeason(season: Option[String], seriesTitle: Option[String]): String = {
    seriesTitle match {
      case Some(_) => checkSeason(season)
      case _ => Global.naColName
    }
  }

  override val transformationUDF: UserDefinedFunction = udf((season: String, seriesTitle: String) => {
    getSeason(Option(season), Option(seriesTitle))
  })
}
