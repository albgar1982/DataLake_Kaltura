package dataLake.core.tests.cucumber.Product.data_warehouse.Playbacks

import com.amazon.deequ.VerificationResult
import dataLake.core.tests.cucumber.Utilities.RunVars
import io.cucumber.scala.{EN, ScalaDsl}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer

class PlaybackActivitySteps extends ScalaDsl with EN {

  var sparkSchema: StructType = _

  var tableName: String = _
  var originSchema: Map[String, String] = _

  var errorString: ArrayBuffer[String] = ArrayBuffer[String]()

  private def checkFulltitle(): String = {

    val validation = RunVars.productSchema.get("fulltitle").get.obj.get("validation").get.obj.get("product").get

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

          errorString += s"Total rows that do not comply the correct format($regex): ${numDistinctRows.count()}"
          errorString += s"The wrong rows: ${numDistinctRows.show(false)}"

        }

      } else { errorString += "The fulltitle column do not have the content types." }

    }

    var messageOfError = ""

    if (errorString.nonEmpty) { messageOfError += s"${errorString.mkString("\n")}" }

    messageOfError

  }

  private def checkMilliseconds(): String = {

    val millisecondsPattern = "(^[1-9][0-9]{3,}$)|(^[0]$)"


    val contentDurationRows = RunVars.currentDF.select("contentduration")
    val playbackTimeRows = RunVars.currentDF.select("playbacktime")

    val numRowsContentDuration = contentDurationRows.count()
    val numRowsPlaybackTime = playbackTimeRows.count()

    if (numRowsContentDuration > 0 && numRowsPlaybackTime > 0){

      val rowsValidatePattern = RunVars.currentDF.filter(col("contentduration").rlike(millisecondsPattern) && col("playbacktime").rlike(millisecondsPattern))

      val rowsNotValidation = RunVars.currentDF.except(rowsValidatePattern)

      val countRowsValidated = rowsValidatePattern.count()

      if (countRowsValidated != numRowsContentDuration || countRowsValidated != numRowsPlaybackTime) {

        errorString += s"Total rows that do not have the correct format: ${rowsNotValidation.count()}"
        errorString += s"The wrong rows: ${rowsNotValidation.show(false)}"

      }

    } else {errorString += "The contentduration column do not have rows."}

    var messageOfError = ""

    if (errorString.nonEmpty) { messageOfError += s"${errorString.mkString("\n")}" }

    messageOfError

  }

  Then("""evaluate the column fulltitle of "playbackactivity" has contents"""){

    val result = checkFulltitle()
    assert(result.isEmpty, s"The following columns do not meet the validation criteria\n$result")

  }

  And("""evaluate the column contentduration and playbacktime is in milliseconds"""){

    val result = checkMilliseconds()
    assert(result.isEmpty, s"The following columns do not meet the validation criteria\n$result")

  }
}
