package dataLake.layers.enriched_raw

import dataLake.core.Launcher
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class catalogueTests extends AnyFlatSpec with should.Matchers {
  "A Stack" should "pop values in last-in-first-out order" in {
    val args = Array(
      "--brandId", "b750c112-bd42-11eb-b0e4-f79ca0b77da6",
      "--environment", "dev",
      "--flavour", "product",
      "--knowledgeUrl", "localhost:XXXX",
      "--layer", "enriched_raw",
      "--executionGroup", "kaltura",
      "--table", "fact_useractivity"
    )

    Launcher.main(args)
  }
}
