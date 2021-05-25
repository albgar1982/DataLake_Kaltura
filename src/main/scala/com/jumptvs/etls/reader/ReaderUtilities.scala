package com.jumptvs.etls.reader

import com.jumptvs.etls.config.GlobalConfiguration

import java.net.URI
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import com.jumptvs.etls.model.Dates
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object ReaderUtilities {

  def buildBrandAndDaydatePaths(config: GlobalConfiguration, sparkSession: SparkSession, basePath: String, isFull: Boolean, startDaydate: LocalDate, endDaydate: LocalDate): Array[String] = {
    var startDate = startDaydate

    println(s"[ReaderUtilities] basePath: ${basePath} is full: ${isFull}")

    var paths = ArrayBuffer[String]()

    if (isFull) {

      paths += s"${basePath}brandid=${config.brandId.get}/"

      return paths.toArray
    }

    while ( {
      !startDate.isAfter(endDaydate)
    }) {
      val currentDaydate = startDate.format(DateTimeFormatter.ofPattern(Dates.yyyyMMdd))
      val candidatePath = s"${basePath}brandid=${config.brandId.get}/daydate=${currentDaydate}/"

      val isDirectory = FileSystem.get(new URI(candidatePath), sparkSession.sparkContext.hadoopConfiguration).isDirectory(new Path(candidatePath))

      if (isDirectory) {
        paths += candidatePath
        println(s"[ReaderUtilities] add path ${candidatePath}")
      }

      startDate = startDate.plusDays(1)
    }

    paths.toArray
  }

}
