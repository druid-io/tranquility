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

package com.metamx.tranquility.storm.common

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.tuple.Fields
import java.{util => ju}
import scala.collection.JavaConverters._

class SimpleSpout[A](inputs: Seq[A]) extends BaseRichSpout
{
  var collector: SpoutOutputCollector = null
  var buf      : List[A]              = null

  override def open(conf: ju.Map[_, _], context: TopologyContext, _collector: SpoutOutputCollector) {
    collector = _collector
    buf = inputs.toList
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(new Fields("event"))
  }

  override def nextTuple() = {
    buf match {
      case x :: xs =>
        collector.emit(List(x.asInstanceOf[AnyRef]).asJava)
        buf = xs
      case Nil =>
    }
  }
}

object SimpleSpout
{
  // Workaround for https://issues.scala-lang.org/browse/SI-9237
  def create[A](inputs: Seq[A]): SimpleSpout[A] = {
    val inputsJava = new ju.ArrayList[A]
    inputs foreach inputsJava.add
    new SimpleSpout(inputsJava.asScala)
  }
}
