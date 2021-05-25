package com.jumptvs.etls.transformations

import com.vitorsvieira.iso.ISOCountry
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import cats.syntax.either._
import com.jumptvs.etls.model.Global

object TransformCountryUDF extends BaseTransformation {

  override val transformationName: String = "getRegionIDOCodeUDF"

  def getISOString(country: String): String = {
    country.toUpperCase
  }

  def getISOCode(country: Option[String]): String = {
    country match {
      case Some(countryISO) => Either.catchNonFatal(ISOCountry(getISOString(countryISO)).value.toLowerCase)
        .getOrElse(Global.naColName)
      case _ => Global.naColName
    }
  }

  def checkAS(country: String): String = {
    country match {
      case "AS" => "at"
      case _ => getISOCode(Option(country))
    }
  }

  def checkBrandID(brandid: String, country: String): String = {
    brandid match {
      case "291bfcf4-5f83-11ea-b6ba-8fc2f567b0a6" => "at"
      case _ => checkAS(country)
    }
  }

  override val transformationUDF: UserDefinedFunction = udf((country: String, brandid: String) =>
    checkBrandID(brandid, country))
}