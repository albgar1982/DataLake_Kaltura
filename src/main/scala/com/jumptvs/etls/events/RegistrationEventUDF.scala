package com.jumptvs.etls.events

import com.jumptvs.etls.model.{Dates, Global}
import com.jumptvs.etls.model.m7.M7
import com.jumptvs.etls.utils.DateUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object RegistrationEventUDF extends BaseEvent {

  override val eventName: String = "registrationEventType"

  private def registrationEventType(startDate: Option[String]): Seq[Event] = {

    val start = startDate.getOrElse("")

    start match {
      case register if startDate.nonEmpty =>

        Seq(Event("Start", "Registration", Global.naColName, false, register))
      case _ => Seq(Event(Global.naColName, Global.naColName, Global.naColName, false, Global.naColName))
    }
  }


  override val eventUDF: UserDefinedFunction = udf(
    (startDate: String) =>
      registrationEventType(Option(startDate)), eventStructType)

}