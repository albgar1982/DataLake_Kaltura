package com.jumptvs.etls.model.m7.sources

trait FrontendUsers extends Epg {
  override val userId = "UserId"
  val name = "Name"
  val login = "Login"
  val registered = "Registered"
  val expires = "Expires"
  val resellerId = "ResellerId"
  val internalName = "internalname"
}
