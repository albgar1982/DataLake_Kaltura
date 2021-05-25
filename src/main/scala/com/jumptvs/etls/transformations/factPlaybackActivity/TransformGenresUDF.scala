package com.jumptvs.etls.transformations.factPlaybackActivity

import com.jumptvs.etls.model.Global
import org.apache.spark.sql.functions.udf
import com.jumptvs.etls.transformations.BaseTransformation
import com.jumptvs.etls.utils.DataUtils
import org.apache.spark.sql.expressions.UserDefinedFunction

object TransformGenresUDF extends BaseTransformation {

  override val transformationName: String = "getGenresUDF"

  private def makeGenres(genres: String): String = {
    val pos = genres.indexOf("(")
    genres.replace('/',',').substring(0,  pos != -1 match {case true => pos case _ => genres.length}).split(",").map(w => w.trim).mkString(",").trim
  }

  private def transformGenres(genres: Option[String]): String = {
    genres match {
      case Some(genresString) if genresString.nonEmpty => makeGenres(genresString)
      case _ => Global.naColName
    }
  }

  override val transformationUDF: UserDefinedFunction = udf((genres: String) =>
    transformGenres(Option(genres))
  )
}
