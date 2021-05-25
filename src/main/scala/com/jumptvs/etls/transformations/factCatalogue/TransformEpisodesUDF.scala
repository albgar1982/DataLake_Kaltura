package com.jumptvs.etls.transformations.factCatalogue

import com.jumptvs.etls.model.Global
import com.jumptvs.etls.transformations.BaseTransformation
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object TransformEpisodesUDF extends BaseTransformation {

  override val transformationName: String = "getEpisodeUDF"

  private def getEpisode(episode: String, seriesTitle: Option[String]): String = {
    seriesTitle match {
      case Some(seriesTitleName) => episode
      case _ => Global.naColName
    }
  }

  override val transformationUDF: UserDefinedFunction = udf((episode: String, seriesTitle: String) => {
    getEpisode(episode, Option(seriesTitle))
  })


}
