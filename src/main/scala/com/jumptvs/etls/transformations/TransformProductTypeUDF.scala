package com.jumptvs.etls.transformations

import com.jumptvs.etls.model.Global
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object TransformProductTypeUDF extends BaseTransformation {

  override val transformationName: String = "getProductTypeUDF"

  private def checkProductType(productType: Option[String]): String = {
    productType match {
      case Some(value) => getProductType(value)
      case _ => Global.naColName
    }
  }

  private def getProductType(productType: String): String = {
    productType match {
      case "BUN" => "Bundle"
      case "STD" => "Standalone"
      case _ => Global.naColName
    }
  }

  override val transformationUDF: UserDefinedFunction = udf((productType: String) =>
    checkProductType(Option(productType)))

}
