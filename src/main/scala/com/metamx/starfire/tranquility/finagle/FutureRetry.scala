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
