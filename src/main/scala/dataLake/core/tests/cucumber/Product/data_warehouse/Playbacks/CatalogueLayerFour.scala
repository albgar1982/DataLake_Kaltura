package dataLake.core.tests.cucumber.Product.data_warehouse.Playbacks

import dataLake.core.Spark
import dataLake.core.layers.PlaybacksColumns
import dataLake.core.tests.cucumber.Product.RawVSLayerSteps
import dataLake.core.tests.cucumber.Product.RawVSLayerSteps.checkComparisionTables
import dataLake.core.tests.cucumber.Utilities.BddSpark.DataTableConvertibleToDataFrame
import dataLake.core.tests.cucumber.Utilities.RunVars.currentDF
import io.cucumber.datatable.DataTable
import io.cucumber.scala.{EN, ScalaDsl}
import org.apache.spark.sql.functions.{col, count, sum}

import scala.collection.mutable.ArrayBuffer

class CatalogueLayerFour extends ScalaDsl with EN {

  var errorString: ArrayBuffer[String] = ArrayBuffer[String]()


  When ("""check that there are a total of {int} catalogues"""){ (totalCatalogue: Int) =>

    val dwhTotalCatalogue = currentDF.count()

    assume(dwhTotalCatalogue == totalCatalogue, s"The total number of catalogues is ${totalCatalogue} and should be ${dwhTotalCatalogue} .")

  }

  And ("""the total number of catalogues per day will be checked""") { (data: DataTable) =>

    val sampleDF = DataTableConvertibleToDataFrame.toDF(data, Spark.configureSparkSession())
    val result = RawVSLayerSteps.checkComparisonOfQuantitiesBetweenTables(currentDF, sampleDF, "daydate")

    assume(result.isEmpty, s"\nThe following errors have been found:\n$result")

  }

  And ("""evaluate some catalogues""") { (data: DataTable) =>

    val sampleDF = DataTableConvertibleToDataFrame.toDF(data, Spark.configureSparkSession())
    val result = checkComparisionTables(currentDF, sampleDF)

    assume(result.isEmpty, s"\n-------\nNot all catalogues exist in the Catalogue table. The non-crossing rows of the sample data are:\n$result")

  }

  Then ("""check that the serieid is according to contentid and contenttype in catalogues""") { (data: DataTable) =>

    val sampleDF = DataTableConvertibleToDataFrame.toDF(data, Spark.configureSparkSession())

    val listContentTypes = Seq("Episode", "Podcast", "Documental", "episode")

    val selectDF = currentDF.select(PlaybacksColumns.contentid, PlaybacksColumns.serieid, PlaybacksColumns.contenttype)
      .where(col(PlaybacksColumns.contenttype).isin(listContentTypes:_*))

    val result = checkComparisionTables(selectDF, sampleDF)

    assume(result.isEmpty, s"\n-------\nNot all catalogues exist in the Catalogue table. The non-crossing rows of the sample data are:\n$result")

  }



}
