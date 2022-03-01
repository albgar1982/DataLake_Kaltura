package com.jumpdd.dataLake.core.layers

import com.jumpdd.dataLake.core.layers.schema.ColumnObject

trait ModelInterface {

  val brandid = "brandid"

  object dateColumns {
    val yeardate = "yeardate"
    val monthdate = "monthdate"
    val daydate = "daydate"
  }
}
