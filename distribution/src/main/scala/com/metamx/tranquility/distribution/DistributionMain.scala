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

package com.metamx.tranquility.distribution

import com.metamx.tranquility.kafka.KafkaMain
import com.metamx.tranquility.server.http.ServerMain

object DistributionMain
{
  def main(args: Array[String]) {
    args.headOption match {
      case Some("server") =>
        ServerMain.main(args.drop(1))

      case Some("kafka") =>
        KafkaMain.main(args.drop(1))

      case _ =>
        System.err.println(s"Usage: ${getClass.getCanonicalName} <command> [args]")
        sys.exit(1)
    }
  }
}
