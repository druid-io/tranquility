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
  private[this] val lock = new AnyRef
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

  override def isAvailable = delegate.isAvailable
}
