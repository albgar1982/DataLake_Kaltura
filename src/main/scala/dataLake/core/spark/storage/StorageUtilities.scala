package dataLake.core.spark.storage

import dataLake.core.KnowledgeDataComputing.executionControl
import dataLake.core.KnowledgeDataComputing.sparkSession
import dataLake.core.KnowledgeDataComputing
import dataLake.core.utilities.Logger
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.col

import java.time.format.DateTimeFormatter

object StorageUtilities extends Logger {

  val hadoopConf = sparkSession.sparkContext.hadoopConfiguration

  def fullRemove(tableName: String, where: String): Unit = {
    val basePath = new Path(s"$where/$tableName")
    val fs = basePath.getFileSystem(hadoopConf)
    fs.delete(basePath, true)
  }

  def deltaRemove(dataFrame: DataFrame, tableName: String, where: String): Unit = {
    var startDate = executionControl.start
    val endDate = executionControl.end

    while ( {
      !startDate.isAfter(endDate)
    }) {
      val currentDaydate = executionControl.start.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
      val pathString = s"$where/$tableName/brandid=${KnowledgeDataComputing.brandId}/daydate=${currentDaydate}/"

      val path = new Path(pathString)
      try {
        val fs = path.getFileSystem(hadoopConf)
        fs.delete(path, true)
        info(s"[Remover] Deleted ${pathString}")
      } catch {
        case exception: Exception => error(s"[Remover] Error when remove data in path '${pathString}'  error ${exception.getLocalizedMessage}")
      }

      startDate = startDate.plusDays(1)
    }
  }
}
