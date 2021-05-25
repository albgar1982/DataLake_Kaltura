package com.jumptvs.etls.writer

import com.jumptvs.etls.config.GlobalConfiguration
import org.apache.spark.sql.DataFrame

trait Writer {
  def write(isFull: Boolean, config: GlobalConfiguration, df: DataFrame, table: String = "", columns: Seq[String]= null)
}
