/*
 * Tranquility.
 * Copyright 2013, 2014, 2015  Metamarkets Group, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
