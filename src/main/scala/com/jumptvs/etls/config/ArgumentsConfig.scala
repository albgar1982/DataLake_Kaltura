package com.jumptvs.etls.config

import cats.syntax.either._

case class ArgumentsConfig(
                            brandId: String,
                            configuration: String,
                            executorMode: String,
                            startDate: String,
                            endDate: String,
                            tables: String,
                            coordinators: String
                          )

case class ArgumentsError(message: String) extends Exception(message)

object ArgumentsConfig {

  def standarizePrefix(string: String): String = {
    if (string.endsWith("/")) {
      string
    } else {
      string + "/"
    }
  }

  def parse(args: Array[String]): Either[Throwable, ArgumentsConfig] = {
    def parseAux(args: List[String], acum: ArgumentsConfig): Either[Throwable, ArgumentsConfig] = args match {
      case "--brandId" :: prefix :: t => parseAux(t, acum.copy(brandId = prefix))
      case "--configuration" :: prefix :: t => parseAux(t, acum.copy(configuration = prefix))
      case "--executorMode" :: prefix :: t => parseAux(t, acum.copy(executorMode = prefix))
      case "--tables" :: prefix :: t => parseAux(t, acum.copy(tables = prefix))
      case "--coordinators" :: prefix :: t => parseAux(t, acum.copy(coordinators = prefix))
      case "--startDate" :: prefix :: t => parseAux(t, acum.copy(startDate = prefix))
      case "--endDate" :: prefix :: t => parseAux(t, acum.copy(endDate = prefix))
      case Nil => acum.asRight
      case param :: _ => ArgumentsError(s"invalid argument $param").asLeft
    }

    for {
      parsed <- parseAux(args.toList, ArgumentsConfig("", "", "delta", "", "", "", "enrichedRaw_datawarehouse"))
      _ <- Either.cond(parsed.brandId != "", (), ArgumentsError("the brand id must be provided --brandId"))
      _ <- Either.cond(parsed.configuration != "", (), ArgumentsError("the configuration name must be provided --configuration"))
    } yield parsed.copy(
      brandId = parsed.brandId,
      configuration = parsed.configuration,
      executorMode = parsed.executorMode,
      tables = parsed.tables,
      coordinators = parsed.coordinators,
      startDate = parsed.startDate,
      endDate = parsed.endDate
    )
  }
}