package com.metamx.starfire.tranquility.test

import com.metamx.common.Granularity
import com.metamx.starfire.tranquility.druid.DruidBeamMaker
import com.simple.simplespec.Spec
import org.joda.time.DateTime
import org.junit.Test

class DruidBeamUnitSpec extends Spec
{

  class A
  {
    @Test def testGenerateFirehoseId()
    {
      val dt = new DateTime("2010-02-03T12:34:56.789Z")
      DruidBeamMaker.generateBaseFirehoseId("x", Granularity.MINUTE, dt, 1) must be("x-34-0001")
      DruidBeamMaker.generateBaseFirehoseId("x", Granularity.HOUR, dt, 1) must be("x-12-0001")
      DruidBeamMaker.generateBaseFirehoseId("x", Granularity.DAY, dt, 1) must be("x-03-0001")
    }
  }

}
