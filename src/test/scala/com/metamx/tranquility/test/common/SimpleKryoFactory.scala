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
package com.metamx.tranquility.test.common

import backtype.storm.serialization.IKryoFactory
import com.esotericsoftware.kryo.Kryo
import com.metamx.common.scala.Predef._
import com.twitter.chill.{KryoSerializer, KryoBase}
import java.{util => ju}
import org.objenesis.strategy.StdInstantiatorStrategy

class SimpleKryoFactory extends IKryoFactory
{
  def getKryo(conf: ju.Map[_, _]) = {
    new KryoBase withEffect {
      kryo =>
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy)
        kryo.setRegistrationRequired(false)
        KryoSerializer.registerAll(kryo)
        kryo.register(Nil.getClass)
        kryo.register(classOf[scala.collection.immutable.::[_]])
        kryo.register(classOf[scala.collection.immutable.List[_]])
    }
  }

  def preRegister(kryo: Kryo, conf: ju.Map[_, _]) {}

  def postRegister(kryo: Kryo, conf: ju.Map[_, _]) {}

  def postDecorate(kryo: Kryo, conf: ju.Map[_, _]) {}
}
