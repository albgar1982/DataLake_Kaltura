package com.jumptvs.etls.processes.dwh

import com.jumptvs.etls.config.GlobalConfiguration
import com.jumptvs.etls.db.DateRange
import com.jumptvs.etls.processes.BaseLoadProcess
import org.apache.spark.sql.DataFrame

class FactCatalogueProcess extends BaseLoadProcess {

  override val tableName: String = "datawarehouse/fact_catalogue"

  override protected def process(df: DataFrame,
                                 dates: DateRange,
                                 globalConfig: GlobalConfiguration,
                                 readerCondition: String = null): DataFrame = {


    df
  }

}
