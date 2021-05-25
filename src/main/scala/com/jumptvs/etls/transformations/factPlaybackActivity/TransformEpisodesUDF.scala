package com.jumptvs.etls.transformations.factPlaybackActivity

import com.jumptvs.etls.model.Global
import com.jumptvs.etls.transformations.BaseTransformation
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object TransformEpisodesUDF extends BaseTransformation {

  override val transformationName: String = "getEpisodesUDF"
  val regex = raw"^(\d+\s?\-\s?\d+|\d+\s?y\s?\d+|\d+)".r
  val episodeDelimiters = raw"Ep\."

  private def search(content: String): String = {
    regex.findFirstIn(content.trim).getOrElse(Global.naColName)
  }

  private def split(episode: Option[String], delimiters: String): Array[String] = {
    episode.getOrElse(Global.naColName).split(delimiters)
  }

  private def runEpisodeExtractor(title: Option[String]): String = {
    val splitedData = split(title, episodeDelimiters)
    if (splitedData.tail.isEmpty) Global.naColName
    else search(splitedData.tail(0))
  }

  private def checkTitleContent(title: Option[String]): String = {
    title match {
      case Some(_) => runEpisodeExtractor(title)
      case _ => Global.naColName
    }
  }

  private def getEpisodes(contentType: String, title: Option[String]): String = {
    contentType match {
      case "Episode" => checkTitleContent(title)
      case _ => Global.naColName
    }
  }

  override val transformationUDF: UserDefinedFunction = udf((contentType: String, title: String) =>
    getEpisodes(contentType, Option(title))
  )

  val transformationCatalogUDF: UserDefinedFunction = udf((title: String) =>
    search(split(Option(title), episodeDelimiters).tail(0))
  )

}
