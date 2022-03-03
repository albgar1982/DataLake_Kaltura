package dataLake.core

import cats.syntax.either._

case class ArgumentsConfig(
                            brandId: String,
                            environment: String,
                            flavour: String,
                            branch: String,
                            kapiConfigUrl: String,
                            kapiConfigToken: String,
                            layer: String,
                            executionGroup: String,
                            table: String,
                            remote: String
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
      case "--branch" :: argumentValue :: t => parseAux(t, acum.copy(branch = argumentValue))
      case "--kapiConfigUrl" :: argumentValue :: t => parseAux(t, acum.copy(kapiConfigUrl = argumentValue))
      case "--kapiConfigToken" :: argumentValue :: t => parseAux(t, acum.copy(kapiConfigToken = argumentValue))
      case "--layer" :: argumentValue :: t => parseAux(t, acum.copy(layer = argumentValue))
      case "--executionGroup" :: argumentValue :: t => parseAux(t, acum.copy(executionGroup = argumentValue))
      case "--table" :: argumentValue :: t => parseAux(t, acum.copy(table = argumentValue))
      case "--remote" :: argumentValue :: t => parseAux(t, acum.copy(remote = argumentValue))
      case Nil => acum.asRight
      case param :: _ => ArgumentsError(s"invalid argument $param").asLeft
    }

    for {
      parsed <- parseAux(args.toList, ArgumentsConfig("", "pro", "default", "default", "https://", "", "", "", "", "true"))
      _ <- Either.cond(parsed.brandId != "", (), ArgumentsError(s"the brand id must be provided --brandId\n  Value received: '${parsed.brandId}'"))
      _ <- Either.cond(Array("true", "false").contains(parsed.remote), (), ArgumentsError(s"the remote must be provided --remote must be one of the following values: 'true' or 'false'\n  Value received: '${parsed.remote}'"))
      _ <- Either.cond(parsed.environment != "", (), ArgumentsError(s"the environment name must be provided --environment\n  Value received: '${parsed.environment}'"))
      _ <- Either.cond(parsed.flavour != "", (), ArgumentsError(s"the flavour name must be provided --flavour\n  Value received: '${parsed.flavour}'"))
      _ <- Either.cond(parsed.kapiConfigUrl != "", (), ArgumentsError(s"the KAPI Configurations URL must be provided --kapiConfigUrl\n  Value received: '${parsed.kapiConfigUrl}'"))
    } yield parsed.copy(
      remote = parsed.remote,
      brandId = parsed.brandId,
      environment = parsed.environment,
      flavour = parsed.flavour,
      branch = parsed.branch,
      kapiConfigUrl = parsed.kapiConfigUrl,
      kapiConfigToken = parsed.kapiConfigToken,
      layer = parsed.layer,
      executionGroup = parsed.executionGroup,
      table = parsed.table
    )
  }
}