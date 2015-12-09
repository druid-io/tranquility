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
  private final long receivedCount, sentCount, failedCount;

  public MessageCounters(long receivedCount, long sentCount, long failedCount)
  {
    this.receivedCount = receivedCount;
    this.sentCount = sentCount;
    this.failedCount = failedCount;
  }

  public long getReceivedCount()
  {
    return this.receivedCount;
  }

  public long getSentCount()
  {
    return this.sentCount;
  }

  public long getFailedCount()
  {
    return this.failedCount;
  }

  public MessageCounters difference(MessageCounters subtrahend)
  {
    if (subtrahend == null) {
      return this;
    }

    return new MessageCounters(
        this.receivedCount - subtrahend.getReceivedCount(),
        this.sentCount - subtrahend.getSentCount(),
        this.failedCount - subtrahend.getFailedCount()
    );
  }

  @Override
  public String toString()
  {
    return String.format("{receivedCount=%s, sentCount=%s, failedCount=%s}", receivedCount, sentCount, failedCount);
  }
}
