package com.jumptvs.etls.db

import java.sql.Timestamp
import java.time.chrono.ChronoLocalDate
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

import cats.effect.IO
import cats.syntax.either._
import com.jumptvs.etls.config.{ArgumentsConfig, GlobalConfiguration}
import doobie.implicits._
import doobie.util.Meta
import doobie.util.fragment.Fragment
import doobie.util.transactor.Transactor

case class ExecutionInfo(start: LocalDate, end: LocalDate, executionType: String)

case class DateRange(start: LocalDate, end: LocalDate)

object Info {
  implicit val javaLocalDateTime: Meta[LocalDateTime] =
    Meta.TimestampMeta.imap(_.toLocalDateTime)(Timestamp.valueOf)


  private[etls] def createCatalogueIfNotExist(implicit xa: Transactor[IO]): Either[Throwable, Int] = {
    createTableIfNotExist("dwh_catalogue")
  }

  private[etls] def createPlaybacksIfNotExist(implicit xa: Transactor[IO]): Either[Throwable, Int] = {
    createTableIfNotExist("dwh_playbackactivity")

  }

  private[etls] def createUserActivityIfNotExist(implicit xa: Transactor[IO]): Either[Throwable, Int] = {
    createTableIfNotExist("dwh_useractivity")
  }

  private[etls] def createSubscriptionProducttypeIfNotExist(implicit xa: Transactor[IO]): Either[Throwable, Int] = {
    createTableIfNotExist("dwh_subscription_producttype")
  }

  private[etls] def createTableIfNotExist(table: String)(implicit xa: Transactor[IO]): Either[Throwable, Int] = {
    val create =
      (fr"CREATE TABLE IF NOT EXISTS " ++ Fragment.const(table) ++
        fr""" (
            |  id SERIAL,
            |  startdate timestamp,
            |  enddate timestamp,
            |  brandid varchar not null
            |)
       """).stripMargin.update.run.transact(xa)

    Either.catchNonFatal(create.unsafeRunSync())
  }

  private def readInfoFromTable(table: String)(arguments: ArgumentsConfig, config: GlobalConfiguration)(implicit xa: Transactor[IO]): Either[Throwable, DateRange] = {
    println("---------- Calling DB ----------")
    val query = fr"select enddate from " ++ Fragment.const(table) ++ Fragment.const(s"where brandid = '${config.brandId.get}'") ++
      fr" order by id DESC limit 1"

    for {
      lastComputedDate <- query
        .query[LocalDateTime]
        .unique
        .transact(xa)
        .attempt
        .unsafeRunSync()
      firstDayToCompute = lastComputedDate.toLocalDate.plusDays(1)
      lastDayToCompute = LocalDate.now().minusDays(1)
      range <- Either.cond(
        !firstDayToCompute.isAfter(lastDayToCompute.asInstanceOf[ChronoLocalDate])
        , DateRange(firstDayToCompute, lastDayToCompute.asInstanceOf[LocalDate]),
        new Exception(s"can't be a range between ${firstDayToCompute.format(DateTimeFormatter.ISO_DATE)} and ${lastDayToCompute.asInstanceOf[LocalDate].format(DateTimeFormatter.ISO_DATE)} in table $table"))
    } yield range
  }
  def readInfoFromTablePostalCode( xa: Transactor[IO]): List[(String, String, Option[String])] = {
    println("---------- Calling Postal Code DB ----------")
    //val (country_code, postal_code, admin_code1) =
           sql"select postal_code, country_code, admin_code1 from geonames"
          .query[(String, String, Option[String])]
          .to[List]
             .transact(xa)
             .unsafeRunSync()
  }

  def readInfoFromTableForex( xa: Transactor[IO]): List[(Int, String, String, Double)] = {
    println("---------- Calling Forex DB ----------")
    sql"select id, pair, daydate, quote from currencies"
      .query[(Int, String, String, Double)]
      .to[List]
      .transact(xa)
      .unsafeRunSync()
  }

  def getCatalogInfo(arguments: ArgumentsConfig, config: GlobalConfiguration)(implicit xa: Transactor[IO]): Either[Throwable, DateRange] = {
    readInfoFromTable("dwh_catalogue")(arguments, config)
  }

  def getPlaybackInfo(arguments: ArgumentsConfig, config: GlobalConfiguration)(implicit xa: Transactor[IO]): Either[Throwable, DateRange] = {
    readInfoFromTable("dwh_playbackactivity")(arguments, config)
  }


  def getSubscriptionProducttypeInfo(arguments: ArgumentsConfig, config: GlobalConfiguration)(implicit xa: Transactor[IO]): Either[Throwable, DateRange] = {
    readInfoFromTable("dwh_subscription_producttype")(arguments, config)
  }


  def getUserActivityInfo(arguments: ArgumentsConfig, config: GlobalConfiguration)(implicit xa: Transactor[IO]): Either[Throwable, DateRange] = {
    readInfoFromTable("dwh_useractivity")(arguments, config)
  }

  def getBillingActivityInfo(arguments: ArgumentsConfig, config: GlobalConfiguration)(implicit xa: Transactor[IO]): Either[Throwable, DateRange] = {
    readInfoFromTable("dwh_billingactivity")(arguments, config)
  }

  def getEPGActivityInfo(arguments: ArgumentsConfig, config: GlobalConfiguration)(implicit xa: Transactor[IO]): Either[Throwable, DateRange] = {
    readInfoFromTable("dwh_epgactivity")(arguments, config)
  }

  private def setInfo(table: String)(brandId: String, startDate: LocalDate, endDate: LocalDate)(implicit xa: Transactor[IO]): Either[Throwable, Int] = {

    val newStart = LocalDateTime.of(startDate.getYear, startDate.getMonthValue, startDate.getDayOfMonth, 0, 0, 0, 0)
    val newEnd = LocalDateTime.of(endDate.getYear, endDate.getMonthValue, endDate.getDayOfMonth, 23, 59, 59, 999999999)

    //println(s"writing date ${newStart.format(DateTimeFormatter.BASIC_ISO_DATE)}, ${newEnd.format(DateTimeFormatter.BASIC_ISO_DATE)} in table $table for brandid ${brandId}" )


    (fr"insert into " ++ Fragment.const(table) ++ fr" (startdate, enddate, brandid) VALUES ($newStart, $newEnd, $brandId)")
      .update
      .run
      .transact(xa)
      .attempt
      .unsafeRunSync()
  }

  def setCatalogInfo(brandId: String, startDate: LocalDate, endDate: LocalDate)(implicit xa: Transactor[IO]): Either[Throwable, Int] =
    setInfo("dwh_catalogue")(brandId, startDate, endDate)

  def setPlaybackInfo(brandId: String, startDate: LocalDate, endDate: LocalDate)(implicit xa: Transactor[IO]): Either[Throwable, Int] =
    setInfo("dwh_playbackactivity")(brandId,startDate, endDate)

  def setUserActivityInfo(brandId: String, startDate: LocalDate, endDate: LocalDate)(implicit xa: Transactor[IO]): Either[Throwable, Int] =
    setInfo("dwh_useractivity")(brandId,startDate, endDate)

  def setSubscriptionProducttypeInfo(brandId: String, startDate: LocalDate, endDate: LocalDate)(implicit xa: Transactor[IO]): Either[Throwable, Int] =
    setInfo("dwh_subscription_producttype")(brandId, startDate, endDate)

  def setBillingActivityInfo(brandId: String, startDate: LocalDate, endDate: LocalDate)(implicit xa: Transactor[IO]): Either[Throwable, Int] =
    setInfo("dwh_billingactivity")(brandId, startDate, endDate)

  def setEPGActivityInfo(brandId: String, startDate: LocalDate, endDate: LocalDate)(implicit xa: Transactor[IO]): Either[Throwable, Int] =
    setInfo("dwh_epgactivity")(brandId, startDate, endDate)

}
