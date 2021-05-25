package com.jumptvs.etls.utils

import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object DataUtils {

  def createSeqFromCommaSeparatedString(content: String): Seq[String] = {
    content.split(",").map(_.trim).toSeq
  }

  def createSeqFromUnderscoreSeparatedString(content: String): Seq[String] = {
    content.split("_").map(_.trim).toSeq
  }

  def cleanNull(value: String): String = {
    Option(value) match {
      case Some(text) => text.trim
      case _ => "NA"
    }
  }

  def exists(path: String)(implicit sparkSession: SparkSession): Boolean = {
    val fs = FileSystem.get(new URI(path), sparkSession.sparkContext.hadoopConfiguration)
    fs.exists(new Path(path))
  }

}
