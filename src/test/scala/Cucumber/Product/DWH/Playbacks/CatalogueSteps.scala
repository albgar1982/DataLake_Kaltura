package Cucumber.Product.DWH.Playbacks

import Cucumber.Utilities.RunVars
import com.amazon.deequ.VerificationResult
import io.cucumber.scala.{EN, ScalaDsl}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.junit.Assert.fail
import org.junit.rules.Verifier

import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}
import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox
import scala.util.Try

class CatalogueSteps extends ScalaDsl with EN {

  var sparkSchema: StructType = _

  var tableName: String = _
  var originSchema: Map[String, String] = _

  type Verifier = DataFrame => VerificationResult


  private def checkTest(): String = {

    val validation = RunVars.productSchema.get("fulltitle").get.obj.get("validation").get.obj.get("product").get

    var errorString = ArrayBuffer[String]()

    validation.obj.foreach{ case (regex, contentTypes)  =>

      val listContents = contentTypes.arr.mkString(",").replaceAll("\"", "").split(",")
      // TODO: using the value to filter by content types


      val rowsByContentTypedDF = RunVars.currentDF
        .where(col("contenttype").isin(listContents:_*))

      val numRowsFiltered = rowsByContentTypedDF.count()

      if (numRowsFiltered > 0) {

        val numRowsValidated = rowsByContentTypedDF.filter(col("fulltitle").rlike(regex))

        val numDistinctRows = rowsByContentTypedDF.except(numRowsValidated)

        val countNumRowsValidated = numRowsValidated.count()

        if (countNumRowsValidated != numRowsFiltered) {

          //errorString += s"Rows that do not comply the correct format($regex): ${numDistinctRows.show(false)}"
          errorString += s"Total rows that do not comply the correct format($regex): ${numDistinctRows.count()}"

        }

      } else { errorString += "The fulltitle column do not have the content types." }

    }

    var messageOfError = ""

    if (errorString.nonEmpty) {
      messageOfError += s"${errorString.mkString("\n")}"
    }

    messageOfError

    //errorString.mkString(", ")

  }

  Then("""evaluate the column fulltitle has contents"""){
    //ASK ALEX
    val result = checkTest()
    assert(result.isEmpty, s"The following columns do not meet the validation criteria\n$result")
    /*
    if (result.nonEmpty){
      fail(s"\n$result")
    }
     */

  }

  
}
