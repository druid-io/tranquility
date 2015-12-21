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

import com.twitter.finagle.Service
import com.twitter.util.{ConstFuture, Throw, Future, Time}

/**
  * Reference-counting Service. Starts with a reference count of 1, which can be incremented by `incrementRefcount`
  * and decremented by `close`. This means you should generally not call `incrementRefcount` immediately after
  * construction. If the reference count reaches zero, the underlying Service is closed.
  *
  * If you use `map`, it will *not* increase the reference count, so only close the newly mapped Service and
  * not the old Service.
  */
class SharedService[A, B](delegate: Service[A, B]) extends Service[A, B]
{
  private[this] val lock     = new AnyRef
  private[this] var refcount = 1

  private[finagle] def incrementRefcount() {
    lock.synchronized {
      if (refcount == 0) {
        throw new IllegalStateException("Reference count has reached zero, cannot increment")
      } else {
        refcount += 1
      }
    }
  }

  def apply(request: A) = delegate.apply(request)

  override def close(deadline: Time) = {
    lock.synchronized {
      if (refcount == 0) {
        new ConstFuture(Throw(new IllegalStateException("Reference count has reached zero already, cannot close")))
      } else {
        refcount -= 1
        if (refcount == 0) {
          delegate.close(deadline)
        } else {
          Future.Done
        }
      }
    }
  }

  override def status = delegate.status
}
