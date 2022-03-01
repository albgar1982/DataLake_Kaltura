package Cucumber.Product.DWH

import Cucumber.Utilities.RunVars
import com.amazon.deequ.VerificationResult
import io.cucumber.scala.{EN, ScalaDsl}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, udf, when}
import org.apache.spark.sql.types.StructType
import com.jumpdd.dataLake.core.layers.PlaybacksColumns._
import Cucumber.Utilities.BddSpark.DataTableConvertibleToDataFrame
import Cucumber.Utilities.RunVars
import Cucumber.Utilities.RunVars.currentDF
import com.jumpdd.dataLake.core.Spark
import com.jumpdd.dataLake.core.layers.UsersColumns.{daydate, event, eventtype, status}
import io.cucumber.datatable.DataTable

import scala.collection.mutable.ArrayBuffer

class UserActivitySteps extends ScalaDsl with EN  {



  var errorString: ArrayBuffer[String] = ArrayBuffer[String]()

  val spark = Spark.configureSparkSession()

  val finalDF = RunVars.currentDF

  Then("""evaluate events have been generated properly""") { (data: DataTable) =>

    val dataDF = DataTableConvertibleToDataFrame.toDF(data, spark)
    val aggregateDF = currentDF
      .select(status,event,eventtype)


  }

  And ("""evaluate trial subscription events have been generated properly""") { (data: DataTable) =>

    val dataDF = DataTableConvertibleToDataFrame.toDF(data, spark)
    val aggregateDF = currentDF
      .select(status,event,eventtype)


  }

  And ("""evaluate registration events have been generated properly""") { (data: DataTable) =>

    val dataDF = DataTableConvertibleToDataFrame.toDF(data, spark)
    val aggregateDF = currentDF
      .select(status,event,eventtype)


  }






}



