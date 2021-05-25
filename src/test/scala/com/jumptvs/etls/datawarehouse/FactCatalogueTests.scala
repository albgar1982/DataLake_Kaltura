package com.jumptvs.etls.datawarehouse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.{BeforeAndAfterAll, EitherValues, FlatSpecLike, Matchers}

class FactCatalogueTests extends FlatSpecLike with Matchers with EitherValues with BeforeAndAfterAll {
  implicit val spark: SparkSession = SparkSession.builder().appName("CatalogueTests").master("local[*]")
    .getOrCreate()

  override def beforeAll(): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    super.beforeAll()
  }

  it should "Read FactCatalogue" in {
    val catalogueDF = spark
      .read
      .parquet("./results/dwh/fact_catalogue")

    catalogueDF.show(200, false)

    val rawCatalogueDF = spark.read
      .json("./seeds/gain/media.json")

    rawCatalogueDF.show(200, false)
  }



  it should "Compute data" in {

    //LECTURA DE LOS CSV DE SEEDS
    val table_medias = spark.read
      .option("multiline", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("header", "true")
      .csv("./seeds/medias.csv")

    val table_distributor_media = spark.read
      .option("header", "true")
      .csv("./seeds/distributor_media.csv")
    val table_distributors = spark.read
      .option("header", "true")
      .csv("./seeds/distributors.csv")
    val table_genre_media = spark.read
      .option("header", "true")
      .csv("./seeds/genre_media.csv")
    val table_genres = spark.read
      .option("header", "true")
      .csv("./seeds/genres.csv")
    val table_seasons = spark.read
      .option("header", "true")
      .csv("./seeds/seasons.csv")


    //LIMPIEZA DE TABLAS Y DISTINCION DE CAMPOS
    val medias = table_medias
      .withColumnRenamed("id", "m_id")
      .withColumnRenamed("type", "m_type")
    val seasons = table_seasons
      .withColumnRenamed("id", "s_id")
      .withColumnRenamed("media_id", "s_media_id")
    val genre_media = table_genre_media
      .withColumnRenamed("media_id", "gm_media_id")
    val genres = table_genres
      .withColumnRenamed("name", "g_name")
      .withColumnRenamed("id", "g_id")
    val distributor_media = table_distributor_media
      .withColumnRenamed("media_id", "dm_media_id")
    val distributors = table_distributors
      .withColumnRenamed("id", "d_id")


    //TABLAS AUXILIARES PARA LA RECURSIVIDAD DE LOS EPISODIOS SOBRE LA TABLA MEDIAS
    val aux_medias = medias
      .withColumnRenamed("m_id", "serie_m_id")
      .withColumnRenamed("title", "serie_title")
    val aux_seasons = seasons
      .withColumnRenamed("s_id", "serie_s_id")
      .withColumnRenamed("s_media_id","serie_s_media_id")
    val aux_distributor_media = distributor_media
      .withColumnRenamed("distributor_id", "serie_distributor_id")
      .withColumnRenamed("dm_media_id", "serie_dm_media_id")
    val aux_distributors = distributors
      .withColumnRenamed("d_id", "serie_d_id")
      .withColumnRenamed("slug", "serie_slug")

    //JOINS
    val catalogueDF = medias
      .select("m_id", "duration", "m_type", "title", "season_id", "episode")

      //seasons + recursividad para el serietitle
      .join(seasons
          .select("season_num", "s_id", "s_media_id")
            .join(aux_medias
              .select("serie_m_id", "serie_title"),
              seasons.col("s_media_id") === aux_medias.col("serie_m_id"), "inner"),
        medias.col("season_id") === seasons.col("s_id"), "left")

      //genre pasando por genre_media + recursividad para los episodios
          .join(genre_media
            .select("genre_id", "gm_media_id")
            .join(genres
              .select("g_name", "g_id"),
              genre_media.col("genre_id") === genres.col("g_id"), "inner"),
            medias.col("m_id") === genre_media.col("gm_media_id"), "left")

      .drop("serie_s_id", "serie_s_media_id") //los elimino ya que como vuelvo a usar aux_seasons abajo se me duplican

      //distributors (network) pasando por distributor_media
      .join(distributor_media
        .select("distributor_id", "dm_media_id")
          .join(distributors
              .select("slug", "d_id"),
            distributor_media.col("distributor_id") === distributors.col("d_id"), "left"),
        medias.col("m_id") === distributor_media.col("dm_media_id"), "left")

      //recursividad de los episodes para sacar el network de la serie a la que pertenecen
      .join(aux_seasons
        .select("serie_s_id", "serie_s_media_id")
        .join(aux_medias
          .select("serie_m_id")
            .join(aux_distributor_media
              .select("serie_distributor_id", "serie_dm_media_id")
              .join(aux_distributors
                .select("serie_d_id", "serie_slug"),
                aux_distributor_media.col("serie_distributor_id") === aux_distributors.col("serie_d_id"), "inner")
              , aux_medias.col("serie_m_id") === aux_distributor_media.col("serie_dm_media_id"), "inner"),
          aux_seasons.col("serie_s_media_id") === aux_medias.col("serie_m_id"), "inner")
        ,medias.col("season_id") === aux_seasons.col("serie_s_id"), "left"
        )


    //COMPUTACION DE LA TABLA
    val enrichedCatalogueDF = catalogueDF
      .withColumn("customerid", lit("fakecustomer"))
      .withColumn("brandid", lit("fakebrand"))
      .withColumn("daydate", lit("20201221"))
      .withColumnRenamed("duration", "contentduration")
      .withColumnRenamed("m_type", "contenttype")
      .withColumn("consumptiontype", lit("NA"))
      .withColumnRenamed("m_id", "contentid")

      .withColumn("serieid",
        when(
          col("contenttype").equalTo("episode"),
            col("s_media_id"))
          .otherwise(col("contentid")))

      //title es igual

      .withColumn("channel", lit("NA"))

      .withColumn("serietitle",
        when(
          col("contenttype").equalTo("episode"),
          col("serie_title"))
          .otherwise(lit("NA")))

      .withColumn("season",
        when(
          col("contenttype").equalTo("episode"),
          col("season_num"))
          .otherwise(lit("NA")))

      .withColumn("episode", when(
        col("episode").isNull,
        lit("NA")
      ).otherwise(col("episode")))

      .withColumn("fulltitle", when(
        col("contenttype").equalTo("episode"),
        concat(
          col("serietitle")
          ,lit(" S-"),col("season_num")
          ,lit(" E-"),col("episode")
          ,lit(" "),col("title"))
      ).otherwise(col("title")))



      .withColumn("network", when(
          col("contenttype").equalTo("episode"), col("serie_slug"))
        .otherwise(col("slug")))

      .withColumn("countrycode", lit("internalcountrycode"))

      .withColumn("regioncode", lit("NA"))

      .groupBy("customerid", "brandid", "daydate", "contentduration", "contenttype", "consumptiontype",
        "contentid", "serieid", "title", "channel", "serietitle", "season", "episode", "fulltitle", "network", "countrycode", "regioncode",
        "season_id", "season_num", "s_id", "s_media_id", "genre_id", "gm_media_id", "serie_title",
        "g_name", "g_id", "distributor_id", "dm_media_id", "slug", "d_id", "serie_s_id", "serie_s_media_id",
        "serie_distributor_id", "serie_dm_media_id", "serie_d_id", "serie_slug")
        .agg((concat_ws(",", collect_list("g_name"))).as("genres"))

      .drop("season_id", "season_num", "s_id", "s_media_id", "genre_id", "gm_media_id", "serie_title",
        "g_name", "g_id", "distributor_id", "dm_media_id", "slug", "d_id", "serie_s_id", "serie_s_media_id", "serie_m_id", "serie_distributor_id", "serie_dm_media_id", "serie_d_id", "serie_slug")
      .distinct()

    enrichedCatalogueDF.show(200)


    /*val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(s"./results/dwh/fact_catalogue"), true)

    enrichedRawDF
      .repartition(col("brandid"))
      .write
      .partitionBy("brandid", "daydate")
      .parquet("./results/dwh/fact_catalogue")*/
  }
}