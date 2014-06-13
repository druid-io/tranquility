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
  var buf                             = inputs

  override def open(conf: ju.Map[_, _], context: TopologyContext, _collector: SpoutOutputCollector) {
    collector = _collector
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
