package dataLake.core.spark.transformations

import dataLake.core.KnowledgeDataComputing.sparkSession
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit, struct}


object CountryCodes {

  val spark = sparkSession
  import spark.implicits._

  def enrichmentPostalCodeColumns(originDF: DataFrame, postalcodeColumnName:String, enrichedColumn: Seq[String], postalCodesDF: DataFrame): DataFrame = {

    /*
    val postalCodeDF = Spark.currentSparkSession
       .read.option("header", "true")
       .options(Map("inferSchema"->"true","delimiter"->"|"))
       .csv(getClass.getResource("/datasets/postalcodes.csv").getPath)
     */


    //val getJson = CountryCodes.convertZipCodesToCountryCode(originDF,postalcodeColumnName)
    //TO DO -- add post to API geolocation
    CountryCodes.selectColumnsFromPostalCode(postalCodesDF,originDF,postalcodeColumnName,enrichedColumn)
  }

  private def selectColumnsFromPostalCode(postalCodeDF: DataFrame,originDF: DataFrame,postalcodeColumnName: String, enrichedColumn: Seq[String] ):DataFrame={

    val enrichedColumnString = enrichedColumn.mkString("\",\"")

    val filteredPostalCodeDF = postalCodeDF.select("postalcode","countrycode", "regioncode","country","region")

    val joinedDF = originDF.join(filteredPostalCodeDF,originDF(postalcodeColumnName)===filteredPostalCodeDF("postalcode")).drop(postalcodeColumnName)

    joinedDF
  }

  private def convertZipCodesToCountryCode(originDF: DataFrame, postalcodeColumnName:String): Unit = {
    val count = originDF.count()
    val countdis = originDF.distinct().count()

    print($"\n $countdis \n")

    val listValues=originDF.select(postalcodeColumnName).distinct().map(f=>f.getString(0))
      .collect.toList


    //val jsonPost= $"{ \n \"dataset\": \"zip code\",\n  \"selectColumns\": [\"countrycode\",\"adminname1\"],\n  \"whereDefinitions\": [\n  {\n  \"postalcode\":$listValues} ] }".toString()


  }

  private def getCountryCode(jsonCountryCode: String,originDF: DataFrame, postalcodeColumnName:String): DataFrame={

    val data = Seq(jsonCountryCode)

    val df=data.toDF("country_json")

    val getValuesDF = df
      .withColumn("countrycode", col("country_json.countrycode"))
      .withColumn("postalcode", col("country_json.postalcode"))
      .drop("country_json")

    val joinedDF = originDF.join(getValuesDF,originDF(postalcodeColumnName)===getValuesDF("postalcode"))

    joinedDF
  }

}
