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
package com.metamx.tranquility.finagle

import com.github.nscala_time.time.Imports._
import com.metamx.common.Backoff
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Time
import com.twitter.util.Timer

object FutureRetry extends Logging
{
  /**
   * Returns a future representing possibly repeated retries of an underlying future creator.
   *
   * @param isTransients retry if any of these predicates return true for a thrown exception
   * @param backoff use this backoff strategy
   * @param quietUntil log exceptions at debug level until this time
   * @param mkfuture call-by-name future
   * @param timer use this timer for scheduling retries
   */
  def onErrors[A](
    isTransients: Seq[Exception => Boolean],
    backoff: Backoff,
    quietUntil: DateTime
  )(mkfuture: => Future[A])(implicit timer: Timer): Future[A] =
  {
    mkfuture rescue {
      case e: Exception if isTransients.exists(_(e)) =>
        new Promise[A] withEffect {
          promise =>
            val now = DateTime.now
            val sleep = backoff.next
            backoff.incr()

            if (now >= quietUntil) {
              log.warn(e, "Transient error, will try again in %,d ms", sleep)
            } else {
              log.debug(e, "Transient error, will try again in %,d ms", sleep)
            }

            timer.schedule(Time.fromMilliseconds(DateTime.now.getMillis + sleep)) {
              promise.become(onErrors(isTransients, backoff, quietUntil)(mkfuture))
            }
        }
    }
  }
}
