package dataLake.core.tests.cucumber.Product.data_warehouse.Users

import dataLake.core.Spark
import dataLake.core.layers.UsersColumns.{event, eventtype, status}
import dataLake.core.tests.cucumber.Utilities.BddSpark.DataTableConvertibleToDataFrame
import dataLake.core.tests.cucumber.Utilities.RunVars
import dataLake.core.tests.cucumber.Utilities.RunVars.currentDF
import io.cucumber.datatable.DataTable
import io.cucumber.scala.{EN, ScalaDsl}

import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}

class UserActivitySteps extends ScalaDsl with EN{

  var  errorString: ArrayBuffer[String] = ArrayBuffer[String]()

  val spark = Spark.configureSparkSession()

  val finalDF = RunVars.currentDF

  Then("""evaluate events have been generated properly""") { (data: DataTable) =>

    val dataDF = DataTableConvertibleToDataFrame.toDF(data, spark)
    val aggregateDF = currentDF
      .select(status,event,eventtype)


  }

  And("""evaluate trial subscription events have been generated properly""") { (data: DataTable) =>

    val dataDF = DataTableConvertibleToDataFrame.toDF(data, spark)
    val aggregateDF = currentDF
      .select(status,event,eventtype)


  }

  And("""evaluate registration events have been generated properly""") { (data: DataTable) =>

    val dataDF = DataTableConvertibleToDataFrame.toDF(data, spark)
    val aggregateDF = currentDF
      .select(status,event,eventtype)


  }

}
