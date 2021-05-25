package com.jumptvs.etls.transformations

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import com.jumptvs.etls.model.m7.M7
import com.jumptvs.etls.utils.DataUtils


object TransformDeviceMappingUDF extends BaseTransformation {

  override val transformationName: String = "getDevicesUDF"
  private val devices = M7.devices
  private val devicesTypes = M7.deviceTypes

  def getDevice(device: String): Int = {
    devices.getOrElse(DataUtils.cleanNull(device), 0)
  }

  def getDeviceType(device: String, os: String): Int = {
    devicesTypes.getOrElse((DataUtils.cleanNull(device), DataUtils.cleanNull(os)), 0)
  }

  override val transformationUDF: UserDefinedFunction = udf((columns: Seq[String]) => {
    columns match {
      case Seq(device, os) => getDeviceType(device, os)
      case Seq(device) => getDevice(device)
    }
  })

}
