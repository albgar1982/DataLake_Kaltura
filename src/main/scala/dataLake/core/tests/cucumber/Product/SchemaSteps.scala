package dataLake.core.tests.cucumber.Product

import dataLake.core.configurations.ConfigurationEngine.getConfigurations
import dataLake.core.tests.cucumber.Utilities.RunVars
import io.cucumber.datatable.DataTable
import io.cucumber.scala.{EN, ScalaDsl, Scenario}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, count, lit, when}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}
import org.junit.Assert._

import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}

class SchemaSteps extends ScalaDsl with EN {

  //var tableConfiguration: =

  Before {}

  var sparkSchema: StructType = _

  var tableName: String = _

  var originSchema: Map[String, String] = _


  And("""we check the schema""") {
    sparkSchema = RunVars.currentDF.schema
    originSchema = sparkSchema.fields.map(field => {
      (field.name, field.dataType.typeName)
    }).toMap

    val config = getConfigurations("DataWarehouse")

    RunVars.productSchema = config(RunVars.currentTableName)("schema")("product").obj
    RunVars.adHocSchema = config(RunVars.currentTableName)("schema")("adhoc").obj
  }

  Then("""we check the columns defined by product""") {
    val messageError = checkSchema(RunVars.productSchema)
    if (messageError.nonEmpty) {
      fail(messageError)
    }
  }

  And("""we check the adhoc columns""") {
    val messageError = checkSchema(RunVars.adHocSchema)
    if (messageError.nonEmpty) {
      fail(messageError)
    }
  }

  And("""check that the formatting for the column names is correct""") {
    val configColumns = (RunVars.productSchema.keySet ++ RunVars.adHocSchema.keySet).filterNot(_.matches("(^[a-z]+$)"))
    val originColumns = originSchema.keySet.filterNot(_.matches("(^[a-z]+$)"))

    var messageOfError = ""
    if (configColumns.nonEmpty) {
      messageOfError += s"""
        The following columns in the knowledge layer do not follow the proper formatting:
          ${configColumns.mkString(", ")}
        """
    }

    if (originColumns.nonEmpty) {
      messageOfError += s"""
        The following columns in the DataFrame do not follow the proper formatting:
          ${originColumns.mkString(", ")}
        """
    }

    if (messageOfError.nonEmpty) {
      messageOfError += "\nDocumentation about appropriate format ~> XX"
      fail(messageOfError)
    }
  }

  And("""we check that there are no columns that are not covered by the knowledge layer""") {
    val configColumns = RunVars.productSchema.keySet ++ RunVars.adHocSchema.keySet
    val noExistColumns = originSchema.keySet.filterNot(configColumns)

    if (noExistColumns.nonEmpty) {
      fail(s"""
        The knowledge layer has no evidence of the following columns:
          ${noExistColumns.mkString(", ")}
        You must add these columns in the knowledge layer. Documentation -> XXX
        """
      )
    }
  }

  When("""check the data in each column""") {

  }

  Then("""hope that there are no null values""") {
    val row = RunVars.currentDF.select(checkNullAndNonAndNanInAllColumns(RunVars.currentDF.columns):_*).first()

    row.schema.fields.foreach(field => {
      val columnIndex = row.schema.fields.indexOf(field)
      val columnValue = row.getLong(columnIndex)

      if (!columnValue.equals(0l)) {
        print(s"${field.name} $columnValue")
      }
    })
  }

  Then("""we expect default values to exist""") {

  }

  private def checkNullAndNonAndNanInAllColumns(columns:Array[String]): Array[Column] = {
    columns.map(column=> {
      count(
        //TODO: Evaluations depending on the type of data (string, Number...)
        when(
          col(column).isNull
            //|| col(column).contains("NULL")
            //|| col(column).contains("null")
          , column
        )
      ).alias(column)
    })
  }

  private def checkSchema(columnsCompare: LinkedHashMap[String, ujson.Value.Value]): String = {
    val nonexistentColumns = ArrayBuffer[String]()
    val incorrectTypeColumns = ArrayBuffer[String]()

    columnsCompare.foreach(columnDef => {
      val columnName = columnDef._1
      val configType = columnDef._2("type").str

      // I have removed the RunVars..
      if (!originSchema.contains(columnName)) {
        nonexistentColumns += s"$columnName:$configType"
      } else {

        val schemaType = originSchema(columnName)

        if (!configType.equals(schemaType)) {
          incorrectTypeColumns += s"$columnName ~> the expected type is '$schemaType' but it is '$configType'"
        }
      }
    })

    var messageOfError = ""

    if (nonexistentColumns.nonEmpty) {
      messageOfError += s"\n  the following columns do not exist in schema ~> ${nonexistentColumns.mkString(", ")}"
    }

    if (incorrectTypeColumns.nonEmpty) {
      messageOfError += s"\n  the following columns does not have the correct data type\n${incorrectTypeColumns.mkString("\n")}"
    }
    messageOfError
  }
}