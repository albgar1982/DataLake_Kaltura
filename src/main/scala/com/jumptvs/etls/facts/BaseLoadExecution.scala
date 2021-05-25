package com.jumptvs.etls.facts

import com.jumptvs.etls.config.GlobalConfiguration
import com.jumptvs.etls.utils.DataFrameUtils
import org.apache.spark.sql.{Column, DataFrame}

trait BaseLoadExecution[Input, Output] {

  def process(input: Input, config: GlobalConfiguration=null): Output

  def cleanAndExplode(df: DataFrame, eventName: String): DataFrame = {
    DataFrameUtils.cleanAndExplode(df, eventName)
  }

}
