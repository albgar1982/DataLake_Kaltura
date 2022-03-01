package com.jumpdd.core.knowledge.configurations.models.DataLake

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

object TableConfig {
  // FAKE IMPLEMENTATION

  implicit val formats = DefaultFormats

  def getConfigurations(fileName: String): DataLakeTables = {
    val jsonString = os.read(os.pwd/"src"/"test"/"resources"/"phil.json")
    val data = ujson.read(jsonString)
    data.value // LinkedHashMap("first_name" -> Str("Phil"), "last_name" -> Str("Hellmuth"), "birth_year" -> Num(1964.0))
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