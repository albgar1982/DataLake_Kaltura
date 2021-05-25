package com.jumptvs.etls.transformations.factPlaybackActivity

import com.jumptvs.etls.model.{Global, MappingContenTypes}
import com.jumptvs.etls.transformations.BaseTransformation
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object TransformContentTypesUDF extends BaseTransformation {

  override val transformationName: String = "getContentTypeUDF"

  private def map(asset: String, seriesExternalid: String): String = {
    asset match {
      case MappingContenTypes.movie1    => "Movie"
      case MappingContenTypes.movie2    => "Movie"
      case MappingContenTypes.broadcast => if (seriesExternalid==null)  "Broadcast" else "Episode"
      case MappingContenTypes.restart   => if (seriesExternalid==null)  "Restart"   else "Episode"
      case MappingContenTypes.replay    => if (seriesExternalid==null)  "Replay"    else "Episode"
      case MappingContenTypes.npvr      => if  (seriesExternalid==null) "Npvr"      else "Episode"
      case MappingContenTypes.episode   => "Episode"
      case _                            => Global.naColName
    }
  }

  private def getContentType(assetType: Option[String], seriesExternalid: String): String = {
    assetType match {
      case Some(asset) => map(asset, seriesExternalid)
      case _ => Global.naColName
    }
  }

  override val transformationUDF: UserDefinedFunction = udf((assetType: String, seriesExternalid: String) =>
    getContentType(Option(assetType), seriesExternalid)
  )
}
