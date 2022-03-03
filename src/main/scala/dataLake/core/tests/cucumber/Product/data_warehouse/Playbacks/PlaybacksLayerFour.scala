package dataLake.core.tests.cucumber.Product.data_warehouse.Playbacks

import dataLake.core.Spark
import dataLake.core.layers.PlaybacksColumns
import dataLake.core.tests.cucumber.Product.RawVSLayerSteps
import dataLake.core.tests.cucumber.Product.RawVSLayerSteps.checkComparisionTables
import dataLake.core.tests.cucumber.Utilities.BddSpark.DataTableConvertibleToDataFrame
import dataLake.core.tests.cucumber.Utilities.RunVars.currentDF
import io.cucumber.datatable.DataTable
import io.cucumber.scala.{EN, ScalaDsl}
import org.apache.spark.sql.{Column, DataFrame, functions}
import org.apache.spark.sql.functions.{col, corr, count, current_date, lit, sum, when}

import scala.collection.mutable.ArrayBuffer

class PlaybacksLayerFour extends ScalaDsl with EN {

  var errorString: ArrayBuffer[String] = ArrayBuffer[String]()


  When ("""check that there are a total of {int} playbacks""") { (totalPlaybacks: Int) =>

    val dwhTotalPaybacks = currentDF.count()

    assume(dwhTotalPaybacks == totalPlaybacks, s"The total number of playbacks is ${totalPlaybacks} and should be ${dwhTotalPaybacks} .")

  }

  And ("""the total number of playbacks per day will be checked""") { (data: DataTable) =>

    val sampleDF = DataTableConvertibleToDataFrame.toDF(data, Spark.configureSparkSession())
    val result = RawVSLayerSteps.checkComparisonOfQuantitiesBetweenTables(currentDF, sampleDF, "daydate")

    assume(result.isEmpty, s"\nThe following errors have been found:\n$result")

  }

  And ("""evaluate some playbacks""") { (data: DataTable) =>

    val sampleDF = DataTableConvertibleToDataFrame.toDF(data, Spark.configureSparkSession())
    val result = checkComparisionTables(currentDF, sampleDF)

    assume(result.isEmpty, s"\n-------\nNot all playbacks exist in the PlaybackActivity table. The non-crossing rows of the sample data are:\n$result")

  }

  Then ("""check the device and devicetype""") { (data: DataTable) =>

    val sampleDF = DataTableConvertibleToDataFrame.toDF(data, Spark.configureSparkSession())
    val aggregateDF = currentDF
      .groupBy(PlaybacksColumns.device, PlaybacksColumns.devicetype)
      .agg(count(col("*")).alias("quantity"))

    val result = checkComparisionTables(aggregateDF, sampleDF)

    assume(result.isEmpty, s"\n-------\nNot all playbacks exist in the PlaybackActivity table. The non-crossing rows of the sample data are:\n$result")

  }

  And ("""check the userid is according to contentid""") { (data: DataTable) =>

    val sampleDF = DataTableConvertibleToDataFrame.toDF(data, Spark.configureSparkSession())
    val selectDF = currentDF.select(PlaybacksColumns.contentid, PlaybacksColumns.userid)

    val result = checkComparisionTables(selectDF, sampleDF)

    assume(result.isEmpty, s"\n-------\nNot all playbacks exist in the PlaybackActivity table. The non-crossing rows of the sample data are:\n$result")

  }

  And ("""check that the serieid is according to contentid and contenttype in playbacks""") { (data: DataTable) =>

    val sampleDF = DataTableConvertibleToDataFrame.toDF(data, Spark.configureSparkSession())

    val listContentTypes = Seq("Episode", "Podcast", "Documental", "episode")

    val selectDF = currentDF.select(PlaybacksColumns.contentid, PlaybacksColumns.serieid, PlaybacksColumns.contenttype)
      .where(col(PlaybacksColumns.contenttype).isin(listContentTypes:_*))

    val result = checkComparisionTables(selectDF, sampleDF)

    assume(result.isEmpty, s"\n-------\nNot all playbacks exist in the PlaybackActivity table. The non-crossing rows of the sample data are:\n$result")

  }


}
