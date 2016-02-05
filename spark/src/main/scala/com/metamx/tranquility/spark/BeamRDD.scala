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
        val sender = beamFactory.tranquilizer.simple(false)
        for (record <- partitionOfRecords) {
          sender.send(record)
        }
        sender.flush()
        log.debug(s"Sent ${sender.sentCount} out of ${sender.receivedCount} events to Druid.")
      }
    }
  }
}

object BeamRDD extends Serializable
{
  implicit def createBeamRDD[T: ClassTag](rdd: RDD[T]): BeamRDD[T] = new BeamRDD(rdd)
}
