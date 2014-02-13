package com.metamx.starfire.tranquility.druid

import org.scala_tools.time.Imports._

trait IndexServiceConfig
{
  def indexRetryPeriod: Period
}
