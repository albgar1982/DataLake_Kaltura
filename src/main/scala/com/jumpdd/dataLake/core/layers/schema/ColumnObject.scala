package com.jumpdd.dataLake.core.layers.schema

import com.jumpdd.dataLake.core.layers.schema.FormatTypes.{ANY, Format}


object FormatTypes extends Enumeration {
  type Format = Value
  val
    LOWER_CASE, UPPER_CASE, DAY_DATE, MONTH_DATE,
    GENDER, BIRTH_DATE, ZIPCODE, EMAIL, USER_EVENT, USER_EVENT_TYPE, USER_STATUS, CANCELLATION_TYPE,
    ANY
    = Value
}

abstract class ColumnObject {
  val name: String
  val defaultValue: Any = null
  val dataType: String
  val format: Format = ANY
}
