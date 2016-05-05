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

package com.metamx.tranquility.kafka.model;

/**
 * Used for passing received, sent, and failed message counts from SimpleTranquilizerAdapter.
 */
public class MessageCounters
{
  private final long receivedCount, sentCount, droppedCount, unparseableCount;

  public MessageCounters(long receivedCount, long sentCount, long droppedCount, long unparseableCount)
  {
    this.receivedCount = receivedCount;
    this.sentCount = sentCount;
    this.droppedCount = droppedCount;
    this.unparseableCount = unparseableCount;
  }

  public long getReceivedCount()
  {
    return this.receivedCount;
  }

  public long getSentCount()
  {
    return this.sentCount;
  }

  public long getDroppedCount()
  {
    return droppedCount;
  }

  public long getUnparseableCount()
  {
    return unparseableCount;
  }

  public MessageCounters difference(MessageCounters subtrahend)
  {
    if (subtrahend == null) {
      return this;
    }

    return new MessageCounters(
        this.receivedCount - subtrahend.getReceivedCount(),
        this.sentCount - subtrahend.getSentCount(),
        this.droppedCount - subtrahend.getDroppedCount(),
        this.unparseableCount - subtrahend.getUnparseableCount()
    );
  }

  @Override
  public String toString()
  {
    return "{" +
           "receivedCount=" + receivedCount +
           ", sentCount=" + sentCount +
           ", droppedCount=" + droppedCount +
           ", unparseableCount=" + unparseableCount +
           '}';
  }
}
