package com.jumptvs.etls.transformations.factUserActivity

import com.jumptvs.etls.transformations.BaseTransformation
import com.vitorsvieira.iso.ISOCountrySubdivision
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import cats.syntax.either._
import com.jumptvs.etls.model.Global

object TransformRegionIsoCodeUDF extends BaseTransformation {

  override val transformationName: String = "getRegionIDOCodeUDF"

  def getISOString(region: String, country: String): String = {
    country.toUpperCase + "-" + region.toUpperCase
  }

  def getISOCode(region: Option[String], country: Option[String]): String = {
    (region, country) match {
      case(Some(regionISO), Some(countryISO)) => Either.catchNonFatal(ISOCountrySubdivision(getISOString(regionISO, countryISO)).value)
        .getOrElse(Global.naColName)
      case _ => Global.naColName
    }
  }

  override val transformationUDF: UserDefinedFunction = udf((region: String, country: String) =>
    getISOCode(Option(region), Option(country)))

}
