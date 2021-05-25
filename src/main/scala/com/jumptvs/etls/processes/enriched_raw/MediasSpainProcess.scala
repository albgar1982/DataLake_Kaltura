package com.jumptvs.etls.processes.enriched_raw

import com.jumptvs.etls.config.GlobalConfiguration
import com.jumptvs.etls.db.DateRange
import com.jumptvs.etls.processes.BaseLoadProcess
import org.apache.spark.sql.DataFrame

class MediasSpainProcess extends BaseLoadProcess {

  override val tableName: String = "media"

  override protected def process(df: DataFrame,
                                 dates: DateRange,
                                 globalConfig: GlobalConfiguration,
                                 readerCondition: String = null): DataFrame = {
    
    df
  }

}
