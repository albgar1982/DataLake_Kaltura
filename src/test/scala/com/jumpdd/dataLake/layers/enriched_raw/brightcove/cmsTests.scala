package com.jumpdd.dataLake.layers.enriched_raw.brightcove

import com.jumpdd.dataLake.{DateRange, ExecutionControl, KnowledgeDataComputing, Launcher}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.time.LocalDate

class cmsTests extends AnyFlatSpec with should.Matchers {
  "A Stack" should "pop values in last-in-first-out order" in {
    val args = Array(
      "--brandId", "285df710-355f-11eb-9917-e71a95af48ce",
      "--environment", "dev",
      "--flavour", "product",
      "--knowledgeUrl", "localhost:XXXX",
      "--layer", "enriched_raw",
      "--executionGroup", "brightcove",
      "--table", "cms"
    )

   // val dateRange = DateRange(LocalDate.now().minusMonths(1), LocalDate.now())

  //  KnowledgeDataComputing.isFull = false
  //  KnowledgeDataComputing.historialMaximum = 24
  //  KnowledgeDataComputing.executionControl = new ExecutionControl(dateRange)

    Launcher.main(args)
  }
}
