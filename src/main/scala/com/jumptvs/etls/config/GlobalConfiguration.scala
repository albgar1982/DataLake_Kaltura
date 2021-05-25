package com.jumptvs.etls.config

import com.typesafe.scalalogging.LazyLogging
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

object GlobalConfiguration extends LazyLogging {

  implicit val formats = DefaultFormats

  def getConfig(brand: String): GlobalConfiguration = {
    val source = scala.io.Source.fromURL(getClass.getResource(s"/$brand.json"))
    parse(source.mkString).extract[GlobalConfiguration]
  }


  def getSeqOfColumnsFromTransformation(transformations: Seq[Transformation],
                                                filterColumn: String): Seq[String] = {
    transformations.filter(_.name.equals(s"${filterColumn}")).head.columns
  }
}

case class GlobalConfiguration(customerIdentifier: Option[String],
                               brandids: Seq[Brands],
                               environment: Option[String],
                               source: Option[Source],
                               target: Option[Target],
                               operations: Seq[Transformation],
                               logLevel: Option[String],
                               brandName: Option[String],
                               processes: Seq[Processes],
                               var brandId: Option[String] = None,
                               var isFull: Boolean = false)

case class Source(path: String,
                  format: String)

case class Target(path: String,
                  format: String)

case class Transformation(name: String,
                          columns: Seq[String])

case class Processes(table: String,
                     days: String)

case class Brands(name: String,
                  id: String)