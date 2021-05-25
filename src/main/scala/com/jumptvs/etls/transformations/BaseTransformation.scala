package com.jumptvs.etls.transformations

trait BaseTransformation {

  val transformationName: String

  val transformationUDF: Any

}
