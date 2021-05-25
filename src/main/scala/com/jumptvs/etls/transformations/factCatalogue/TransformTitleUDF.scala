package com.jumptvs.etls.transformations.factCatalogue

import com.jumptvs.etls.model.Global
import com.jumptvs.etls.transformations.BaseTransformation
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object TransformTitleUDF extends BaseTransformation {

  override val transformationName: String = "getTitleUDF"

  val regex = raw"^(\d+\s?\-\s?\d+|\d+\s?y\s?\d+|\d+)".r
  val episodeDelimiters = raw"Ep\."

  private def search(content: String): String = {
    regex.findFirstIn(content.trim).getOrElse(Global.naColName)
  }

  private def split(episode: String, delimiters: String): Array[String] = {
    episode.split(delimiters)
  }

  def getCatalogueTitle(title: String, seriesTitle: Option[String]): String = {
    seriesTitle match {
      case Some(seriesTitleName) => search(split(seriesTitleName, episodeDelimiters).tail(0))
      case _ => title
    }
  }

  override val transformationUDF: UserDefinedFunction = udf((title: String, seriesTitle: String) => {
    getCatalogueTitle(title, Option(seriesTitle))
  })

}
