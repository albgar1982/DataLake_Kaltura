package com.jumptvs.etls.transformations.factPlaybackActivity

import com.jumptvs.etls.model.Global
import com.jumptvs.etls.transformations.BaseTransformation
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object TransformGroupContentTypesUDF extends BaseTransformation {

  import com.jumptvs.etls.utils._

  override val transformationName: String = "getGroupContentTypesUDF"

  private def getContentType(playOfferIdValue: String): String = {
    playOfferIdValue.toInt match {
      case isLargerThanZero() => "SVOD"
      case isEqualToZero() => "TVOD"
      case _ => Global.naColName
    }
  }

  private def checkPlayOfferId(playOfferIdValue: String): Boolean = {
    playOfferIdValue match {
      case isDigit() => true
      case _ => false
    }
  }

  private def groupCountentType(assetType: String, playOfferIdValue: String): String = {

    assetType match {
      case "s"  => "Linear TV"
      case "q"  => "Restart"
      case "r"  => "Replay"
      case "v"  => "PVR"
      case _    =>   if (checkPlayOfferId(playOfferIdValue))
                          getContentType(playOfferIdValue)
                      else
                          Global.naColName
    }
  }

  private def getGroupContentTypes(assetType: Option[String], playOfferId: Option[String]): String = {
    (assetType, playOfferId) match {
      case(Some(assetType), Some(playOfferId)) => groupCountentType(assetType, playOfferId)
      case _ => Global.naColName
    }
  }

  override val transformationUDF: UserDefinedFunction = udf((assetType: String, playOfferId: String) =>
    getGroupContentTypes(Option(assetType), Option(playOfferId))
  )

}
