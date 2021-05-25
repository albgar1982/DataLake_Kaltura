package com.jumptvs.etls.model.m7

import com.jumptvs.etls.model.m7.sources.{BackendUsers, Billing, Catalog, DimSubscriptions, Epg, FrontendUsers, Playbacks, PostalCode, Subscribers, Forex}
import com.jumptvs.etls.model.m7.maps.{Brands, Device}

object M7 extends Forex with PostalCode with BackendUsers with Subscribers with FrontendUsers with Playbacks with Billing with DimSubscriptions with Catalog with Epg with Brands with Device {
  val dateFormat = "yyyy-MM-dd HH:mm:ss"
  val subscriptionDateFormat = "dd/MM/yyyy HH:mm:ss"
  val brandid = "brandid"
  val brandid_SkylinkSK = "b4d2c9ea-bc2f-11ea-9d26-9b25df47fc99"
  val brandid_SkylinkCZ = "b4d297f4-bc2f-11ea-9d25-3fc7a647ba19"

}
