/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.metamx.tranquility.spark

import com.metamx.common.scala.Logging
import com.metamx.tranquility.beam.Beam
import com.metamx.tranquility.tranquilizer.MessageDroppedException
import com.metamx.tranquility.tranquilizer.Tranquilizer
import com.twitter.util.Return
import com.twitter.util.Throw
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import org.apache.spark.rdd.RDD
import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
  * This class provides any RDD the ability to propagate events to Druid.
  *
  * @param rdd rdd that should be propagated to druid.
  */
class BeamRDD[T: ClassTag](rdd: RDD[T]) extends Logging with Serializable
{
  def propagate(beamFactory: BeamFactory[T]) = {
    rdd.foreachPartition {
      partitionOfRecords => {
        val beam = beamFactory.makeBeam
        val sender = BeamRDD.tranquilizer(beam)
        val received = new AtomicLong
        val sent = new AtomicLong
        val exception = new AtomicReference[Throwable]

        for (record <- partitionOfRecords) {
          sender.send(record) respond {
            case Return(_) => sent.incrementAndGet()
            case Throw(e: MessageDroppedException) => // Suppress
            case Throw(e) => exception.compareAndSet(null, e)
          }
        }
        sender.flush()
        log.debug(s"Sent ${sent.get()} out of ${received.get()} events to Druid.")
      }
    }
  }
}

object BeamRDD extends Serializable
{
  implicit def createBeamRDD[T: ClassTag](rdd: RDD[T]): BeamRDD[T] = new BeamRDD(rdd)

  // We want a single Tranquilizer per Beam, this is the only way that comes to mind of making that happen...
  private val tranquilizers: java.util.IdentityHashMap[Beam[_], Tranquilizer[_]] = new java.util.IdentityHashMap

  private def tranquilizer[A](beam: Beam[A]): Tranquilizer[A] = {
    tranquilizers.synchronized {
      tranquilizers.get(beam) match {
        case null =>
          val t = Tranquilizer.create(beam)
          tranquilizers.put(beam, t)
          t.start()
          t

        case t => t.asInstanceOf[Tranquilizer[A]]
      }
    }
  }
}
