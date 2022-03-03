package dataLake.core.tests.cucumber.Product

import io.cucumber.scala.{EN, ScalaDsl}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, count, lit, when}
import org.apache.spark.sql.types.StructType
import org.junit.Assert._

import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstrainableDataTypes._
import com.amazon.deequ.constraints.{ConstrainableDataTypes, ConstraintStatus}
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
import dataLake.core.tests.cucumber.Utilities.RunVars
import org.apache.spark
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset

import scala.collection.mutable
import scala.reflect.runtime.universe._
import scala.reflect.runtime.currentMirror
import scala.runtime.AbstractFunction1
import scala.tools.reflect.ToolBox
import scala.util.Try
import scala.util.matching.Regex

class BasicDataChecksSteps extends ScalaDsl with EN {

  //var tableConfiguration: =

  Before {}

  //code to build verifier from DF that has a 'Constraint' column
  type Verifier = DataFrame => VerificationResult

  def generateVerifier(constrains: Map[String, String]): Try[Verifier] = {

    val constrainsCode = constrains.map({ case (constrainDef, columnName) => {
      val fixContrainDef = constrainDef.replaceAll("'", "\"")
      s"""Check(Error, "$columnName")$fixContrainDef"""
    }}).mkString(",\n  ")


    val verifierSrcCode = s"""{
                             |import com.amazon.deequ.constraints.ConstrainableDataTypes
                             |import com.amazon.deequ.{VerificationResult, VerificationSuite}
                             |import com.amazon.deequ.checks.CheckLevel.Error
                             |import org.apache.spark.sql.DataFrame
                             |import scala.util.matching.Regex
                             |import com.amazon.deequ.checks.Check
                             |
                             |val checks = Seq(
                             |  $constrainsCode
                             |)
                             |
                             |(data: DataFrame) => VerificationSuite().onData(data).addChecks(checks).run()
                             |}
    """.stripMargin.trim

    //println(s"Verification function source code:\n$verifierSrcCode\n")

    compile[Verifier](verifierSrcCode)
  }

  /** Compiles the scala source code that, when evaluated, produces a value of type T. */
  def compile[T](source: String): Try[T] =
    Try {
      val toolbox = currentMirror.mkToolBox()
      val tree = toolbox.parse(source)
      val compiledCode = toolbox.compile(tree)
      compiledCode().asInstanceOf[T]
    }

  private def buildDeequTest(): Verifier = {

    val spark = RunVars.sparkSession

    val contrains = scala.collection.mutable.Map[String, String]()

    // REGEX PATTERNS
    val emptyString = """(.*\\S.*)""".r
    val uuidFormat = "(^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$)".r
    val dayDateFormat = "(^([0-9]{4})(0[1-9]|1[0-2])(0[1-9]|[1-2][0-9]|3[0-1])$)".r
    val monthDateFormat = "(^([0-9]{4})(0[1-9]|1[0-2])[0][1]$)".r
    val yearDateFormat = """(^\\d{4}[0][1][0][1]$)""".r
    val hourDateFormat = "(^([0-9]{4})(0[1-9]|1[0-2])(0[1-9]|[1-2][0-9]|3[0-1])(2[0-3]|[01][0-9])$)".r
    val contentIdFormat = "(^[A-Z]{4}[0-9]{4}$)".r
    val millisecondsFormat = "(^[1-9][0-9]{3,}$)|(^[0]$)".r
    val countryCodeFormat = "(^[a-z]{2}$)".r
    val regionCodeFormat = "(^[a-z]{2}-([a-z]{2}|[0-9]{2})$)".r


    RunVars.productSchema.foreach(columnDef => {
      val typeOfData = columnDef._2("type")
      val columnName = columnDef._1
      val validation = columnDef._2("validation")

      validation.obj.foreach{ case (key, value) =>

        val constraintDescriptionKey = s"$key"

        key match {
          case "containedIn" => {
            val arrayValues = validation(key).arr.mkString(",")

            contrains += (s".isContainedIn('$columnName', Array($arrayValues), Check.IsOne, Option('$constraintDescriptionKey'))" -> columnName)
          }

          case "columnProperties" => {
            value.arr.foreach( property => {

              val constraintDescription = s"${property.str}"

              property.str match {
                case "notNull" => contrains += (s".isComplete('$columnName', Option('$constraintDescription'))" -> columnName)
                case "formatUUID" => contrains += (s".hasPattern('$columnName', new Regex('$uuidFormat'), Check.IsOne, Option('$constraintDescription'))" -> columnName)
                case "nonEmpty" => contrains += (s".hasPattern('$columnName', new Regex('$emptyString'), Check.IsOne, Option('$constraintDescription'))" -> columnName)
                case "formatDayDate" => contrains += (s".hasPattern('$columnName', new Regex('$dayDateFormat'), Check.IsOne, Option('$constraintDescription'))" -> columnName)
                case "formatMonthDate" => contrains += (s".hasPattern('$columnName', new Regex('$monthDateFormat'), Check.IsOne, Option('$constraintDescription'))" -> columnName)
                case "formatYearDate" => contrains += (s".hasPattern('$columnName', new Regex('$yearDateFormat'), Check.IsOne, Option('$constraintDescription'))" -> columnName)
                case "nonNegative" => contrains += (s".isNonNegative('$columnName', Check.IsOne, Option('$constraintDescription'))" -> columnName)
                case "formatHourDate" => contrains += (s".hasPattern('$columnName', new Regex('$hourDateFormat'), Check.IsOne, Option('$constraintDescription'))" -> columnName)
                case "formatContentId" => contrains += (s".hasPattern('$columnName', new Regex('$contentIdFormat'), Check.IsOne, Option('$constraintDescription'))" -> columnName)
                case "formatCountryCode" => contrains += (s".hasPattern('$columnName', new Regex('$countryCodeFormat'), Check.IsOne, Option('$constraintDescription'))" -> columnName)
                case "formatRegionCode" => contrains += (s".hasPattern('$columnName', new Regex('$regionCodeFormat'), Check.IsOne, Option('$constraintDescription'))" -> columnName)
                //case "formatMilliseconds" => contrains += (s".hasPattern('$columnName', new Regex('$millisecondsFormat'), Check.IsOne, Option('$constraintDescription'))" -> columnName)

                case _ => println(s"Not implemented '$property'")
              }
            })
          }

          case "pattern" => {
            contrains += (s".hasPattern('$columnName', new Regex($value), Check.IsOne, Option('$constraintDescriptionKey'))" -> columnName)
          }

          case _ => println(s"No implemented $key")
        }
      }
    })

    generateVerifier(contrains.toMap).get
  }

  def runDeequTest(): String = {

    val verifier = buildDeequTest()
    val result = verifier(RunVars.currentDF)

    val resulDF = VerificationResult.checkResultsAsDataFrame(RunVars.sparkSession, result).cache()

    resulDF.where(col("check_status").equalTo("Error"))
      .groupBy("check")
      .agg(collect_list(col("constraint")))
      .collect()
      .map(row => s"${row.get(0)}\n ${row.getList(1)}").mkString("\n")
  }

  Then("""we check the value of columns""") {
    val result = runDeequTest()
    var messageError = ""
    assert(result.isEmpty, s"The following columns do not meet the validation criteria\n$result")
    /*
    if (result.isEmpty) {
      messageError += s"The following columns do not meet the validation criteria\n$result"
      println(messageError)
    }
     */
  }
}