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
package com.metamx.starfire.tranquility.finagle

import com.metamx.common.Backoff
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.twitter.util.{Promise, Time, Timer, Future}
import org.scala_tools.time.Implicits._

object FutureRetry extends Logging
{
  /**
   * Returns a future representing possibly repeated retries of an underlying future creator.
   *
   * @param mkfuture call-by-name future
   * @param isTransients retry if any of these predicates return true for a thrown exception
   * @param timer use this timer for scheduling retries
   */
  def onErrors[A](
    mkfuture: => Future[A],
    isTransients: Seq[Exception => Boolean],
    backoff: Backoff = Backoff.standard()
  )(implicit timer: Timer): Future[A] = {
    mkfuture rescue {
      case e: Exception if isTransients.exists(_(e)) =>
        new Promise[A] withEffect {
          promise =>
            val next = backoff.next
            backoff.incr()
            log.warn(e, "Transient error, will try again in %s ms", next)
            timer.schedule(Time.now + next.toDuration) {
              promise.become(onErrors(mkfuture, isTransients, backoff))
            }
        }
    }
  }
}
