package com.jumpdd.dataLake.layers.data_warehouse.fact_useractivity

import com.jumpdd.dataLake.core.layers.{ModelInterface, UsersColumns}
import com.jumpdd.dataLake.core.layers.schema.ColumnObject
import com.jumpdd.dataLake.core.layers.schema.FormatTypes.{DAY_DATE, LOWER_CASE, USER_EVENT, USER_EVENT_TYPE, USER_STATUS}

/**
 *
 */

object factUserActivityModel extends ModelInterface {

  val trialPeriod = "trialperiod"

  val timestampstart_date = "timestampstart_date"
  val timestampend_date = "timestampend_date"
  val timestamptrialenddate = "timestamptrialenddate"







}
