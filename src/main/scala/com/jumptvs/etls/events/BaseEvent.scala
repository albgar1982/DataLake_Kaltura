package com.jumptvs.etls.events

import com.jumptvs.etls.model.Tmp
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{ArrayType, BooleanType, StringType, StructType}

trait BaseEvent {

  val eventName: String

  val eventUDF: UserDefinedFunction

  val eventStructType: ArrayType = ArrayType(new StructType()
    .add(Tmp.jumpEventType, StringType)
    .add(Tmp.jumpEvent, StringType)
    .add(Tmp.jumpStatus, StringType)
    .add(Tmp.jumpMassiveDeactivation, BooleanType)
    .add(Tmp.jumpEventDate, StringType))

}

case class Event(eventType: String,
                 event: String,
                 status: String,
                 massiveDeactivation: Boolean,
                 eventDate: String)


