package com.jumpdd.dataLake

import com.jumpdd.core.knowledge.configurations.{DataLakeSchema, StorageSourceEntity}
import com.jumpdd.dataLake.core.ArgumentsConfig
import com.jumpdd.dataLake.core.spark.StorageEngine
import com.jumpdd.dataLake.core.utilities.Logger
import org.apache.spark.sql.DataFrame
import util.control.Breaks._

import scala.reflect.runtime.{universe => ru}
import scala.reflect.internal.pickling.ByteCodecs

object Coordinator extends Logger {

  def run(argumentsConfiguration: ArgumentsConfig, dataLakeConfig: ujson.Value.Value): Unit = {

    // TODO: Pending refactor
    dataLakeConfig.obj.foreach(layer => {
      val layerName = layer._1
      breakable {
        if (argumentsConfiguration.isAdhocExecution() && layerName != argumentsConfiguration.layer) {
          debug(s"Skip layer '$layerName'")
          break()
        }

        layer._2.obj.foreach(group => {
          val executionGroup = group._1
          breakable {
            if (argumentsConfiguration.isAdhocExecution() && executionGroup != argumentsConfiguration.executionGroup) {
              debug(s"Skip Execution Group '$executionGroup'")
              break()
            }

            group._2.obj.foreach(table => {
              val tableName = table._1

              breakable {
                if (argumentsConfiguration.isAdhocExecution() && tableName != argumentsConfiguration.table) {
                  debug(s"Skip table '$tableName'")
                  break()
                }

                val subPath = table._2("storage")("target")("subPath").str
                headMessage(s"[Coordinator][$layerName][$executionGroup] $tableName")
                val dataSources = readSources(table._2("storage")("sources"))

                val finalDF = dynamicCallComputation(layerName, subPath, tableName, dataSources)
                val targetPath = s"./results/$layerName/$subPath"

                StorageEngine
                  .remove(finalDF, tableName, targetPath, true)

                StorageEngine
                  .write(finalDF, tableName, targetPath)
              }
            })
          }
        })
      }
    })
  }

  private def readSources(sources: ujson.Value.Value): Map[String, DataFrame] = {
    val dataFrames = scala.collection.mutable.Map[String, DataFrame]()

    sources.obj.foreach(source => {
      val sourceDF = StorageEngine
        .read(source._1, source._2.str, null)
      dataFrames += (source._1 -> sourceDF)
    })

    dataFrames.toMap
  }

  private def dynamicCallComputation(layerName: String, subPath: String, tableName: String, dataSources: Map[String, DataFrame]): DataFrame = {

    // TODO: Improve
    var fixSubPath = subPath.replaceAll("/", ".")

    if (fixSubPath.length > 0) {
      fixSubPath = s".$fixSubPath."
    } else {
      fixSubPath = "."
    }

    val namespace = s"com.jumpdd.dataLake.layers.$layerName$fixSubPath$tableName.${tableName}Computation"

    val m = ru.runtimeMirror(getClass.getClassLoader)
    val module = m.staticModule(namespace)
    val im = m.reflectModule(module)
    val method = im.symbol.info.decl(ru.TermName("process")).asMethod

    val objMirror = m.reflect(im.instance)
    objMirror.reflectMethod(method)(dataSources).asInstanceOf[DataFrame]
  }
}