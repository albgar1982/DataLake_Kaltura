package com.jumpdd.dataLake.layers.data_warehouse.fact_catalogue

import com.jumpdd.dataLake.core.layers.{ModelInterface, UsersColumns}
import com.jumpdd.dataLake.core.layers.schema.ColumnObject
import com.jumpdd.dataLake.core.layers.schema.FormatTypes.{DAY_DATE, LOWER_CASE, USER_EVENT, USER_EVENT_TYPE, USER_STATUS}



object fact_catalogueModel extends ModelInterface {
  val tmpEndDate = "tmpEndDate"
}
