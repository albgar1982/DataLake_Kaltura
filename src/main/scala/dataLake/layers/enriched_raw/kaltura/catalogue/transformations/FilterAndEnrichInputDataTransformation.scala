package dataLake.layers.enriched_raw.kaltura.catalogue.transformations

import dataLake.layers.enriched_raw.kaltura.catalogue.catalogueModel.{assetDuration, catalog_start, identifier, media_id, media_type, meta_name, meta_name_value, name}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, regexp_replace}

object FilterAndEnrichInputDataTransformation {
  def process(originDF: DataFrame): DataFrame = {

      val factCatalogueFields = originDF.
      select(
        col(media_id),
        originDF.col(catalog_start),
        originDF.col(identifier),
        originDF.col(media_type),
        originDF.col(name),
        originDF.col(assetDuration)(0),
        originDF.col(meta_name),
        originDF.col(meta_name_value)
      )


    factCatalogueFields


  }

}
