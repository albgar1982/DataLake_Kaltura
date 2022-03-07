package dataLake.layers.data_warehouse.catalogue.transformations

import dataLake.core.layers.PlaybacksColumns.{brandid, consumptiontype, contentduration, contentid, contenttype, countrycode, daydate, episode, fulltitle, regioncode, season, serietitle, title}
import dataLake.layers.data_warehouse.catalogue.catalogueModel.{genreposition, providerposition}
import dataLake.layers.enriched_raw.kaltura.catalogue.catalogueModel.{assetDuration, catalog_start, identifier, media_id, media_type, meta_name, meta_name_value, name}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array_contains, array_position, col, concat, element_at, lit, substring, when}

object ChangeNameAndAddColumnsTransformations {
  def process(originDF: DataFrame): DataFrame = {

    val changeNameDF = changeName(originDF)
    val addColumsDF = addColumns(changeNameDF)

    addColumsDF
  }



  def changeName(originDF: DataFrame): DataFrame = {
  val factCatalogueFields = originDF.
    select(
      col(media_id).alias(contentid),
      originDF.col(catalog_start),
      originDF.col(identifier),
      originDF.col(media_type).alias(contenttype),
      originDF.col(name).alias(title),
      originDF.col(assetDuration)(0).alias(contentduration),
      originDF.col(meta_name),
      originDF.col(meta_name_value)
    )

    factCatalogueFields

}

  def addColumns(originDF: DataFrame): DataFrame = {

    val factCatalogueFields = originDF
      .withColumn(providerposition, when(array_contains(col(meta_name), "Provider"),
      array_position(col(meta_name), "Provider")).otherwise(""))
      .withColumn(genreposition, when(array_contains(col(meta_name), "Genre"),
        array_position(col(meta_name), "Genre")).otherwise(""))

    val ValuesInteger = factCatalogueFields.withColumn(providerposition, col(providerposition).cast("Int"))
      .withColumn(genreposition, col(genreposition).cast("Int"))

    val obtainProviderGenre = ValuesInteger
      .withColumn("provider", when(col(providerposition).isNotNull,
        lit(element_at(col(meta_name_value), ValuesInteger.col(providerposition)))))
      .withColumn("genre", when(col(genreposition).isNotNull,
        lit(element_at(col(meta_name_value), ValuesInteger.col(genreposition)))))
      .drop(col(meta_name)).drop(col(meta_name_value)).drop(col(providerposition)).drop(col(genreposition))


    val obtainValue = obtainProviderGenre.withColumn("provider", when(col("provider").isNotNull, lit(col("provider.value._VALUE")(0))))
      .withColumn("genre", when(col("genre").isNotNull, lit(col("genre.value._VALUE")(0))))



    val factCatalogueCreateColumns = obtainValue
      .withColumn(brandid, lit("b750c112-bd42-11eb-b0e4-f79ca0b77da6"))
      .withColumn(daydate,
        concat(substring(col(catalog_start), 7, 4),
          substring(col(catalog_start), 4, 2),
          substring(col(catalog_start), 1, 2)))
      .withColumn(consumptiontype, lit("VOD"))
      .withColumn(serietitle, lit("NA"))
      .withColumn(season, lit("NA"))
      .withColumn(episode, lit("NA"))
      .withColumn(contentduration,
        when(col(contentduration).isNull, lit("0"))
          .otherwise(col(contentduration) * 1000))
      .withColumn(fulltitle, col("title"))
      .withColumn(countrycode, lit("LK"))
      .withColumn(regioncode, lit("NA"))
      .drop(col(catalog_start))


    factCatalogueCreateColumns

  }


}