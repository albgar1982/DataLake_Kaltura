package com.jumptvs.etls.model.m7.maps

trait Device {
  val devices: Map[String, Int] = Map(
    "Apple TV" -> 15,
    "stb" -> 6,
    "Android Phone" -> 2,
    "Android Tablet" -> 5,
    "ctv" -> 11,
    "iPad" -> 4,
    "iPhone" -> 1,
    "iPod touch" -> 12,
    "TV Streamer" -> 13,
    "tvstick" -> 10,
    "web" -> 45
  )

  val deviceTypes: Map[(String, String), Int] = Map(
    ("android", "Android Phone") -> 3,
    ("android", "Android Tablet") -> 4,
    ("android", "ctv") -> 5,
    ("ctv", "ctv") -> 5,
    ("ctv", "unknown") -> 5,
    ("ctv", "NA") -> 5,
    ("ios", "iPad") -> 4,
    ("ios", "iPhone") -> 3,
    ("ios", "iPod touch") -> 4,
    ("stb", "stb") -> 1,
    ("stb", "unknown") -> 1,
    ("stb", "NA") -> 1,
    ("tvstick", "TV Streamer") -> 2,
    ("tvstick", "tvstick") -> 2,
    ("tvstick", "NA") -> 2,
    ("web", "ctv") -> 7,
    ("web", "unknown") -> 7,
    ("web", "web") -> 7,
    ("web", "NA") -> 7
  )
}
