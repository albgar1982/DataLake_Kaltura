package com.jumptvs.etls.transformations.factUserActivity

import com.jumptvs.etls.model.Global
import com.jumptvs.etls.transformations.BaseTransformation
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object TransformGenderUDF extends BaseTransformation {

  override val transformationName: String = "getGenderUDF"

  private def genders(gender: String): String = {
    gender.toLowerCase match {
      case "male" => "male"
      case "female" => "female"
      case _ => Global.naColName
    }
  }

  def getGender(gender: Option[String]): String = {
    gender match {
      case Some(value) => genders(value)
      case _ => Global.naColName
    }
  }

  override val transformationUDF: UserDefinedFunction = udf((gender: String) =>
    getGender(Option(gender)))

}
