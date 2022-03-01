package com.jumpdd.dataLake.core.utilities

import com.jumpdd.core.utilities.LoggerUtilities

import java.text.SimpleDateFormat
import java.util.{Date, SimpleTimeZone}

trait Logger {

  private val infoSeverity = "info"
  private val debugSeverity = "debug"
  private val noticeSeverity = "notice"
  private val warningSeverity = "warning"
  private val errorSeverity = "err"
  private val criticalSeverity = "critic"
  private val alertSeverity = "alert"
  private val emergencySeverity = "emerg"

  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd H:mm:s.S")
  dateFormat.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"))

  def debug(message: String)(implicit namespace: sourcecode.Pkg, fileName: sourcecode.FileName, method: sourcecode.Name, line: sourcecode.Line): Unit = {
    sendLog(debugSeverity, message, namespace.value, fileName.value, method.value, line.value)
  }

  def info(message: String)(implicit namespace: sourcecode.Pkg, fileName: sourcecode.FileName, method: sourcecode.Name, line: sourcecode.Line): Unit = {
    sendLog(infoSeverity, message, namespace.value, fileName.value, method.value, line.value)
  }

  def notice(message: String)(implicit namespace: sourcecode.Pkg, fileName: sourcecode.FileName, method: sourcecode.Name, line: sourcecode.Line): Unit = {
    sendLog(noticeSeverity, message, namespace.value, fileName.value, method.value, line.value)
  }

  def warning(message: String)(implicit namespace: sourcecode.Pkg, fileName: sourcecode.FileName, method: sourcecode.Name, line: sourcecode.Line): Unit = {
    sendLog(warningSeverity, message, namespace.value, fileName.value, method.value, line.value)
  }

  def error(message: String)(implicit namespace: sourcecode.Pkg, fileName: sourcecode.FileName, method: sourcecode.Name, line: sourcecode.Line): Unit = {
    sendLog(errorSeverity, message, namespace.value, fileName.value, method.value, line.value)
  }

  def alert(message: String)(implicit namespace: sourcecode.Pkg, fileName: sourcecode.FileName, method: sourcecode.Name, line: sourcecode.Line): Unit = {
    sendLog(alertSeverity, message, namespace.value, fileName.value, method.value, line.value)
  }

  def headMessage(message: String): Unit = {
    print(s"""
#######
  ${message}
#######\n\n"""
    )
  }

  private def sendLog(severity: String, message: String, namespace: String, fileName: String, method: String, line: Int): Unit = {

    val cleanNameSpace = namespace.replaceAll("com.jumpdd.", "")
    val currentDate = new Date()
    val dateFormatted = dateFormat.format(currentDate)

    println(s"$dateFormatted $severity [$cleanNameSpace][$fileName][$method:$line] $message")

    val tags = Map(
      "brandId" -> "",
      "severity" -> severity
    )

    val fields = Map(
      "namespace" -> namespace,
      "timestamp" -> currentDate.getTime,
      "message" -> message,
      "version" -> "",
      "fileName" -> fileName,
      "methodName" -> method,
      "codeLine" -> line
    )

    LoggerUtilities.send(tags, fields)
  }
}
