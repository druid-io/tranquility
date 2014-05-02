/*
 * Tranquility.
 * Copyright (C) 2013, 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package com.metamx.tranquility.test

import com.metamx.common.Granularity
import com.metamx.tranquility.druid.DruidBeamMaker
import com.simple.simplespec.Spec
import org.joda.time.DateTime
import org.junit.Test

class DruidBeamTest extends Spec
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
