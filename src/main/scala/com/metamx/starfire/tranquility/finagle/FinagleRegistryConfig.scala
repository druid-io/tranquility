package com.metamx.starfire.tranquility.finagle

import org.scala_tools.time.Imports._

trait FinagleRegistryConfig
{
  def finagleHttpTimeout: Period

  def finagleHttpConnectionsPerHost: Int
}
