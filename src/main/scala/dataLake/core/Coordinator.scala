package dataLake.core

import dataLake.core.spark.StorageEngine
import dataLake.core.utilities.Logger
import org.apache.spark.sql.DataFrame

import java.lang.reflect.InvocationTargetException
import scala.reflect.runtime.universe.ModuleSymbol
import scala.reflect.runtime.{universe => ru}
import scala.util.control.Breaks._

object Coordinator extends Logger {

  def run(argumentsConfiguration: ArgumentsConfig, dataLakeConfig: ujson.Value.Value, brandConfig: ujson.Value.Value): Unit = {

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
            if (argumentsConfiguration.isAdhocExecution() && executionGroup != argumentsConfiguration.executionGroup && argumentsConfiguration.executionGroup.nonEmpty) {
              debug(s"Skip Execution Group '$executionGroup'")
              break()
            }

            group._2.obj.foreach(table => {
              val tableName = table._1

              breakable {
                if (argumentsConfiguration.isAdhocExecution() && tableName != argumentsConfiguration.table && argumentsConfiguration.table.nonEmpty) {
                  debug(s"Skip table '$tableName'")
                  break()
                }

                KnowledgeDataComputing.mapping = table._2.obj.contains("mapping") match {
                  case true => table._2("mapping").obj
                  case false => null
                }

                val partialTargetPath = table._2("storage")("target")("path").str
                val storageEnvironment = table._2("storage")("target")("environment").str
                val storage = brandConfig.obj("aws")("storage")(storageEnvironment).str

                headMessage(s"[Coordinator][$layerName][$executionGroup] $tableName")
                val dataSources = readSources(table._2("storage")("sources"), brandConfig)

                val finalDF = dynamicCallComputation(layerName, partialTargetPath, tableName, dataSources)
                val targetPath = s"$storage$layerName/$partialTargetPath"
                debug(s"targetPath -> $targetPath")

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

  private def readSources(sources: ujson.Value.Value, brandConfig: ujson.Value.Value): Map[String, DataFrame] = {
    val dataFrames = scala.collection.mutable.Map[String, DataFrame]()

    sources.obj.foreach(source => {
      var schema: String = null
      val environment: String = source._2.obj("environment").str

      if (source._2.obj.contains("schema")) {
        schema = source._2.obj("schema").toString()
      }

      val basePath = brandConfig.obj("aws")("storage")(environment).str
      val dataPath =  source._2.obj("path").str

      val fullPath = s"$basePath$dataPath"

      debug(fullPath)
      val dataFrame = StorageEngine
        .read(source._1, fullPath, null, null, schema)
      dataFrames += (source._1 -> dataFrame)
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

    val runtimeMirror = ru.runtimeMirror(getClass.getClassLoader)
    var module: ModuleSymbol = null
    var namespace = s"com.jumpdd.dataLake.layers.$layerName$fixSubPath$tableName.${tableName}Computation"

    try {
      module = runtimeMirror.staticModule(namespace)
    } catch {
      case _: ScalaReflectionException => warning(s"Computer object in namespace '$namespace' cannot be found")
    } finally {
      if (module == null) {
        try {
          namespace = s"com.jumpdd.dataLake.core.layers.$layerName$fixSubPath$tableName.${tableName}Computation"
          module = runtimeMirror.staticModule(namespace)
        } catch {
          case _: ScalaReflectionException => error(s"Computer object in namespace '$namespace' cannot be found")
        }
      }
    }

    //val modules = runtimeMirror.staticModule(namespace)

    val im = runtimeMirror.reflectModule(module)
    val method = im.symbol.info.decl(ru.TermName("process")).asMethod

    val objMirror = runtimeMirror.reflect(im.instance)

    try {
      objMirror.reflectMethod(method)(dataSources).asInstanceOf[DataFrame]
    } catch {
      case exception: InvocationTargetException =>
        error(exception.getTargetException.getMessage)
        throw new Exception
    }
  }
}