package dataLake.core.configurations.models.DataLake

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

object TableConfig {
  // FAKE IMPLEMENTATION

  implicit val formats = DefaultFormats

  def getConfigurations(fileName: String): DataLakeTables = {
    val source = scala.io.Source.fromURL(getClass.getResource(s"./knowledme/DataLake/$fileName.json"))
    parse(source.mkString).extract[DataLakeTables]
  }
}

case class DataLakeTables(fact_useractivity: Seq[Table])


case class Table(

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