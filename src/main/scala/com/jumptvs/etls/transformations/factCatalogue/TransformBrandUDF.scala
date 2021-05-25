package com.jumptvs.etls.transformations.factCatalogue

import com.jumptvs.etls.model.Global
import com.jumptvs.etls.model.m7.M7
import com.jumptvs.etls.transformations.BaseTransformation
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object TransformBrandUDF extends BaseTransformation {

  override val transformationName: String = "getBrandMetadataUDF"

  private def getDataDate(key: String, brand: String): String = {
    M7.ids.getOrElse(brand, Global.naColName) match {
      case Global.naColName => Global.naColName
      case _ => M7.ids(brand)(key)
    }
  }

  override val transformationUDF: String => UserDefinedFunction = key => udf((brand: String) =>
    getDataDate(key, brand))

}
