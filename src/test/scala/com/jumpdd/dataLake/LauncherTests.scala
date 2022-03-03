package com.jumpdd.dataLake

import dataLake.core.{ArgumentsConfig, Launcher}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class LauncherTests extends AnyFlatSpec with should.Matchers {
  "A Stack" should "pop values in last-in-first-out order" in {
    val args = Array(
      "--brandId", "285df710-355f-11eb-9917-e71a95af48ce",
      "--environment", "dev",
      "--flavour", "product",
      "--knowledgeUrl", "localhost:XXXX"
    )

    val arguments = ArgumentsConfig.parse(args)
    Launcher.main(args)
  }
}
