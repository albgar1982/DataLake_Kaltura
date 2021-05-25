package com.jumptvs.etls.remover

import java.time.format.DateTimeFormatter
import com.jumptvs.etls.db.DateRange
import com.jumptvs.etls.model.Dates
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path}
import org.apache.spark.sql.SparkSession

object Remover {
  def remove(sparkSession: SparkSession, basePath: String, dates: DateRange = null, isFull: Boolean = true): Unit = {
    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration

    println(s"[Remover] basePath ${basePath}")

    if (isFull) {
      removeFullMode(hadoopConf, basePath)
    } else {
      removeDeltaMode(hadoopConf, basePath, dates)
    }
  }

  private def removeFullMode(hadoopConf: Configuration, basePath: String): Unit = {
    try {
      val path = new Path(s"${basePath}")
      val fs = path.getFileSystem(hadoopConf)
      fs.delete(path, true)
      println(s"[Remover] Deleted ${basePath}")
    } catch {
      case exception: Exception => println(s"[Remover] Error when remove data in path '${basePath}'  error ${exception.getLocalizedMessage}")
    }
  }

  private def removeDeltaMode(hadoopConf: Configuration, basePath: String, dates: DateRange): Unit = {
    var startDate = dates.start
    while ( {
      !startDate.isAfter(dates.end)
    }) {
      val currentDaydate = startDate.format(DateTimeFormatter.ofPattern(Dates.yyyyMMdd))
      val pathString = s"${basePath}/daydate=${currentDaydate}/"
      val path = new Path(pathString)
      try {
        val fs = path.getFileSystem(hadoopConf)
        fs.delete(path, true)
        println(s"[Remover] Deleted ${pathString}")
      } catch {
        case exception: Exception => println(s"[Remover] Error when remove data in path '${basePath}'  error ${exception.getLocalizedMessage}")
      }

      startDate = startDate.plusDays(1)
    }
  }
}
