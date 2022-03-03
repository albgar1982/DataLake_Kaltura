package dataLake.core.tests.cucumber.Product.data_warehouse.Users

import dataLake.core.Spark
import dataLake.core.tests.cucumber.Product.RawVSLayerSteps
import dataLake.core.tests.cucumber.Product.RawVSLayerSteps.checkComparisionTables
import dataLake.core.tests.cucumber.Utilities.BddSpark.DataTableConvertibleToDataFrame
import dataLake.core.tests.cucumber.Utilities.RunVars.currentDF
import io.cucumber.datatable.DataTable
import io.cucumber.scala.{EN, ScalaDsl}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, countDistinct, row_number}

import scala.collection.mutable.ArrayBuffer

class UserActivityLayerFour extends ScalaDsl with EN {

  var errorString: ArrayBuffer[String] = ArrayBuffer[String]()

  val rowNumber = "rn"

  val w1 = Window.partitionBy("userid").orderBy(col("userid").desc)


  When ("""check that there are a total of {int} events""") { (TotalEvents: Int) =>

    val dwhTotalEvents = currentDF.count()

    assume(dwhTotalEvents == TotalEvents, s"The total number of playbacks is ${TotalEvents} and should be ${dwhTotalEvents} .")

  }

  And ("""check that there are a total of {int} users""") { (totalUsers: Int) =>

    val dwhTotalUsers = currentDF.groupBy("userid").count().where(col("userid") > 1).count()

    assume(dwhTotalUsers == totalUsers, s"The total number of playbacks is ${totalUsers} and should be ${dwhTotalUsers} .")

  }

  And ("""the total number of events per user""") { (data: DataTable) =>

    val sampleDF = DataTableConvertibleToDataFrame.toDF(data, Spark.configureSparkSession())
    val result = RawVSLayerSteps.checkComparisonOfQuantitiesBetweenTables(currentDF, sampleDF, "userid")

    assume(result.isEmpty, s"\nThe following errors have been found:\n$result")

  }

  And ("""the total number of events per day will be checked""") { (data: DataTable) =>

    val sampleDF = DataTableConvertibleToDataFrame.toDF(data, Spark.configureSparkSession())
    val result = RawVSLayerSteps.checkComparisonOfQuantitiesBetweenTables(currentDF, sampleDF, "daydate")

    assume(result.isEmpty, s"\nThe following errors have been found:\n$result")

  }

  Then("""check the possible events logic between event, eventtype and status""") { (data: DataTable) =>

    val sampleDF = DataTableConvertibleToDataFrame.toDF(data, Spark.configureSparkSession())
    val aggregateDF = currentDF
      .groupBy("event", "eventtype", "status")
      .agg(count("*").alias("quantity"))

    val result = checkComparisionTables(aggregateDF, sampleDF)

    assume(result.isEmpty, s"\n-------\nNot all events exist in the UserActivity table. The non-crossing rows of the sample data are:\n$result")

  }

  And("""evaluate some users"""){ (data: DataTable) =>

    val sampleDF = DataTableConvertibleToDataFrame.toDF(data, Spark.configureSparkSession())
    val result = checkComparisionTables(currentDF, sampleDF)

    assume(result.isEmpty, s"\n-------\nNot all users exist in the UserActivity table. The non-crossing rows of the sample data are:\n$result")

  }

  Then("""check that the paymentmethod receives all the methods""") { (data: DataTable) =>

    val sampleDF = DataTableConvertibleToDataFrame.toDF(data, Spark.configureSparkSession())
    val aggregateDF = currentDF
      .withColumn(rowNumber, row_number().over(w1))
      .where(col(rowNumber) === 1)
      .drop(rowNumber)
      .groupBy("paymentmethod")
      .agg(count("*").alias("quantity"))

    val result = checkComparisionTables(aggregateDF, sampleDF)

    assume(result.isEmpty, s"\n-------\nNot all paymentmethods exist in the UserActivity table. The non-crossing rows of the sample data are:\n$result")

  }

  And("""check the total of cancellationtypes""") { (data: DataTable) =>

    val sampleDF = DataTableConvertibleToDataFrame.toDF(data, Spark.configureSparkSession())
    val aggregateDF = currentDF
      .where(col("eventtype").equalTo("End") && col("event").equalTo("Subscription"))
      .groupBy("cancellationtype").agg(count("*").alias("quantity"))

    val result = checkComparisionTables(aggregateDF, sampleDF)

    assume(result.isEmpty, s"\n-------\nNot all cancellation types exist in the UserActivity table. The non-crossing rows of the sample data are:\n$result")

  }

  And("""check the total of producttypes""") { (data: DataTable) =>


    val sampleDF = DataTableConvertibleToDataFrame.toDF(data, Spark.configureSparkSession())

    val aggregateDF = currentDF
      .withColumn(rowNumber, row_number().over(w1))
      .where(col(rowNumber) === 1)
      .drop(rowNumber)
      .groupBy("producttype")
      .agg(count("*").alias("quantity"))

    val result = checkComparisionTables(aggregateDF, sampleDF)

    assume(result.isEmpty, s"\n-------\nNot all producttypes exist in the UserActivity table. The non-crossing rows of the sample data are:\n$result")

  }

  And("""check the total of genders""") { (data: DataTable) =>

    val sampleDF = DataTableConvertibleToDataFrame.toDF(data, Spark.configureSparkSession())

    val aggregateDF = currentDF
      .withColumn(rowNumber, row_number().over(w1))
      .where(col(rowNumber) === 1)
      .drop(rowNumber)
      .groupBy("gender")
      .agg(count("*").alias("quantity"))

    val result = checkComparisionTables(aggregateDF, sampleDF)

    assume(result.isEmpty, s"\n-------\nNot all genders exist in the UserActivity table. The non-crossing rows of the sample data are:\n$result")

  }

  And("""evaluate the regions and country codes"""){ (data: DataTable) =>


    val sampleDF = DataTableConvertibleToDataFrame.toDF(data, Spark.configureSparkSession())

    val aggregateDF = currentDF
      .withColumn(rowNumber, row_number().over(w1))
      .where(col(rowNumber) === 1)
      .drop(rowNumber)
      .groupBy("country", "countrycode", "region", "regioncode")
      .agg(count("*").alias("quantity"))

    val result = checkComparisionTables(aggregateDF, sampleDF)

    assume(result.isEmpty, s"\n-------\nNot all regions or country codes exist in the UserActivity table. The non-crossing rows of the sample data are:\n$result")

  }

  And("""check the birthday dates"""){ (data: DataTable) =>

    val sampleDF = DataTableConvertibleToDataFrame.toDF(data, Spark.configureSparkSession())

    val aggregateDF = currentDF
      .withColumn(rowNumber, row_number().over(w1))
      .where(col(rowNumber) === 1)
      .drop(rowNumber)
      .groupBy("birth")
      .agg(count("*").alias("quantity"))

    val result = checkComparisionTables(aggregateDF, sampleDF)

    assume(result.isEmpty, s"\n-------\nNot all birthdays exist in the UserActivity table. The non-crossing rows of the sample data are:\n$result")

  }

  Then("""evaluate the subscriptionid"""){ (data: DataTable) =>

    val sampleDF = DataTableConvertibleToDataFrame.toDF(data, Spark.configureSparkSession())

    val aggregateDF = currentDF
      .groupBy("subscriptionid")
      .agg(countDistinct("subscriptionid").alias("quantity"))

    val result = checkComparisionTables(aggregateDF, sampleDF)

    assume(result.isEmpty, s"\n-------\nNot all subscriptionid exist in the UserActivity table. The non-crossing rows of the sample data are:\n$result")

  }

}
