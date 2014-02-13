package com.metamx.starfire.tranquility

import com.metamx.common.scala.Predef._
import com.twitter.util.{Duration => TwitterDuration, Timer, Future}
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpMethod, HttpVersion, DefaultHttpRequest}
import org.joda.time.Duration
import org.scala_tools.time.Implicits._
import org.slf4j.bridge.SLF4JBridgeHandler

package object finagle
{
  implicit def jodaDurationToTwitterDuration(duration: Duration) = TwitterDuration.fromMilliseconds(duration.millis)

  lazy val FinagleLogger = java.util.logging.Logger.getLogger("finagle") withEffect {
    _ =>
      SLF4JBridgeHandler.removeHandlersForRootLogger()
      SLF4JBridgeHandler.install()
  }

  // Call-by-name so we can recreate a future from scratch when it fails
  class FutureRetryOps[A](mkfuture: => Future[A])(implicit timer: Timer)
  {
    def retryWhen(isTransients: (Exception => Boolean)*) = FutureRetry.onErrors(mkfuture, isTransients)
  }

  implicit def FutureRetryOps[A](mkfuture: => Future[A])(implicit timer: Timer) = new FutureRetryOps(mkfuture)

  def HttpPost(path: String) = {
    new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, path) withEffect {
      req =>
        decorateRequest(req)
    }
  }

  def HttpGet(path: String) = {
    new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, path) withEffect {
      req =>
        decorateRequest(req)
    }
  }

  private[this] def decorateRequest(req: HttpRequest) = {
    // finagle-http doesn't set the host header, and we don't actually know what server we're hitting
    req.headers.set("Host", "127.0.0.1")
    req.headers.set("Accept", "*/*")
  }
}
