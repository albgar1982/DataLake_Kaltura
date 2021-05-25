package com.jumptvs.etls.transformations

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object TransformFullTitleUDF extends BaseTransformation {

  override val transformationName: String = "getFullTitleUDF"

  private def getFullTitle(network: String, seriesTitle: String, season: String, episode: String, title: String): String = {
    network + "-" + seriesTitle + " S-" + season +  " E-" + episode + " "  + title
  }

  override val transformationUDF: UserDefinedFunction = udf((network: String, seriesTitle: String, season: String, episode: String, title: String) =>
    Option(seriesTitle) match {
      case Some(serieTitle) => getFullTitle(network, serieTitle, season, episode, title)
      case _ => title
    }
  )
}
