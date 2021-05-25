package com.jumptvs.etls.utils

import com.typesafe.scalalogging.LazyLogging

object LoggingUtils extends LazyLogging {

  def columns(columns: Seq[String], table: String): Unit = {
    val cols = columns.mkString(", \n")
    logger.info(s"Writing table $table with following columns: $cols")
  }
}
