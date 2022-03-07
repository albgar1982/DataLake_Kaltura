package dataLake.layers.data_warehouse.playbacks.transformations

import dataLake.core.layers.PlaybacksColumns.{brandid, contentid, contenttype}
import dataLake.layers.enriched_raw.kaltura.catalogue.catalogueModel.media_id
import dataLake.layers.enriched_raw.kaltura.playbacksessions.playbacksessionsModel.media_type_category
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object JoinPlaybacksAndCatalogue {

  def process(playbacksessionsDF: DataFrame,factCatalogueDF: DataFrame): DataFrame = {

    val renameDF = playbacksessionsDF.withColumn(contentid, col(media_id))
      .withColumnRenamed(media_type_category,contenttype)


    val playbackCatalogueJoinedDF = renameDF
      .join(factCatalogueDF, Seq(contentid), "left")
      .filter(col(brandid).isNotNull)
      .dropDuplicates()
      .drop(factCatalogueDF(contenttype))

      playbackCatalogueJoinedDF


  }


}
