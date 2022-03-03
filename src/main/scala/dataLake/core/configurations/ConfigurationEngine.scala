package dataLake.core.configurations

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._


object ConfigurationEngine {
  // FAKE IMPLEMENTATION

  implicit val formats = DefaultFormats

  def getConfigurations(brandId: String, environment: String, flavour: String): DataLakeSchema = {
    val source = scala.io.Source.fromURL(getClass.getResource(s"/etl.json"))
    parse(source.mkString).extract[DataLakeSchema]
  }

  def getConfigurations(fileName: String): ujson.Value.Value = {
    val source = scala.io.Source.fromFile(s"knowledgme/DataLake/$fileName.json")
    ujson.read(source.mkString)
  }

  def getResourceConfiguration(environment: String, fileName: String): ujson.Value.Value = {
    val source = scala.io.Source.fromResource(s"$environment/$fileName.json")
    ujson.read(source.mkString)
  }
}

case class DataLakeSchema(
                     enrichedRaw: Seq[TableEntity],
                     dataWarehouse: Seq[TableEntity],
                     kpis: Seq[TableEntity]
                   )

case class TableEntity(
                        name: String,
                        storage: StorageEntity,
                        partitions: Seq[String]
                        )
case class StorageEntity(
                  sources: Array[StorageSourceEntity],
                  target: StorageTargetEntity
                  )

case class StorageSourceEntity(
                        name: String,
                        path: String
                        )

case class StorageTargetEntity(
                                subPath: String
                              )
