package com.jumptvs.etls.model.m7.sources

trait Forex {
  val pairCurrency = "pair"
  val daydateCurrency = "daydate"
  val quoteCurrency = "quote"
  val idCurrency = "id"
  val quotes = Seq("CZKEUR", "HUFEUR", "RONEUR")
}