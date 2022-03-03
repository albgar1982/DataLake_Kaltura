package dataLake.core.layers

import dataLake.core.layers.schema.ColumnObject

trait ModelInterface {

  val brandid = "brandid"
  val contentid = "contentid"

  object dateColumns {
    val yeardate = "yeardate"
    val monthdate = "monthdate"
    val hourdate = "hourdate"
    val daydate = "daydate"
  }
}
