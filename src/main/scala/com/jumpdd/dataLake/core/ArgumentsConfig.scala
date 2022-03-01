package com.jumpdd.dataLake.core

import cats.syntax.either._

case class ArgumentsConfig(
                            brandId: String,
                            environment: String,
                            flavour: String,
                            knowledgeUrl: String,
                            layer: String,
                            executionGroup: String,
                            table: String
                          ) {
  def isAdhocExecution(): Boolean = {
    layer.nonEmpty || executionGroup.nonEmpty || table.nonEmpty
  }
}

case class ArgumentsError(message: String) extends Exception(message)

object ArgumentsConfig {

  def parse(args: Array[String]): Either[Throwable, ArgumentsConfig] = {
    def parseAux(args: List[String], acum: ArgumentsConfig): Either[Throwable, ArgumentsConfig] = args match {
      case "--brandId" :: argumentValue :: t => parseAux(t, acum.copy(brandId = argumentValue))
      case "--environment" :: argumentValue :: t => parseAux(t, acum.copy(environment = argumentValue))
      case "--flavour" :: argumentValue :: t => parseAux(t, acum.copy(flavour = argumentValue))
      case "--knowledgeUrl" :: argumentValue :: t => parseAux(t, acum.copy(knowledgeUrl = argumentValue))
      case "--layer" :: argumentValue :: t => parseAux(t, acum.copy(layer = argumentValue))
      case "--executionGroup" :: argumentValue :: t => parseAux(t, acum.copy(executionGroup = argumentValue))
      case "--table" :: argumentValue :: t => parseAux(t, acum.copy(table = argumentValue))
      case Nil => acum.asRight
      case param :: _ => ArgumentsError(s"invalid argument $param").asLeft
    }

    for {
      parsed <- parseAux(args.toList, ArgumentsConfig("", "pro", "main", "https://", "", "", ""))
      _ <- Either.cond(parsed.brandId != "", (), ArgumentsError(s"the brand id must be provided --brandId\n  Value received: '${parsed.brandId}'"))
      _ <- Either.cond(Array("stg", "pro", "dev").contains(parsed.environment), (), ArgumentsError(s"the environment name must be provided --environment must be one of the following values: stg, pro or 'dev'\n  Value received: '${parsed.environment}'"))
      _ <- Either.cond(parsed.flavour != "", (), ArgumentsError(s"the flavour name must be provided --flavour\n  Value received: '${parsed.flavour}'"))
      _ <- Either.cond(parsed.knowledgeUrl != "", (), ArgumentsError(s"the Knowledge URL must be provided --knowledgeUrl\n  Value received: '${parsed.knowledgeUrl}'"))
    } yield parsed.copy(
      brandId = parsed.brandId,
      environment = parsed.environment,
      flavour = parsed.flavour,
      knowledgeUrl = parsed.knowledgeUrl,
      layer = parsed.layer,
      executionGroup = parsed.executionGroup,
      table = parsed.table
    )
  }
}