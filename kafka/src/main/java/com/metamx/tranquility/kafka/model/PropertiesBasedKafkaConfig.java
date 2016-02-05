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

import com.google.common.collect.ImmutableSet;
import com.metamx.tranquility.config.PropertiesBasedConfig;
import org.skife.config.Config;
import org.skife.config.Default;

/**
 * Configuration object which extends Tranquility configuration with Kafka specific parameters.
 */
public abstract class PropertiesBasedKafkaConfig extends PropertiesBasedConfig
{
  public PropertiesBasedKafkaConfig()
  {
    super(
        ImmutableSet.of(
            "kafka.group.id",
            "kafka.zookeeper.connect",
            "consumer.numThreads",
            "commit.periodMillis"
        )
    );
  }

  @Config("kafka.group.id")
  @Default("tranquility-kafka")
  public abstract String getKafkaGroupId();

  @Config("kafka.zookeeper.connect")
  public abstract String getKafkaZookeeperConnect();

  @Config("consumer.numThreads")
  @Default("-1")
  public abstract Integer getConsumerNumThreads();

  @Config("topicPattern")
  @Default("(?!)")
  public abstract String getTopicPattern();

  @Config("useTopicAsDataSource")
  @Default("false")
  public abstract Boolean useTopicAsDataSource();

  @Config("topicPattern.priority")
  @Default("1")
  public abstract Integer getTopicPatternPriority();

  @Config("commit.periodMillis")
  @Default("15000")
  public abstract Integer getCommitPeriodMillis();

  @Config("reportDropsAsExceptions")
  @Default("false")
  public abstract Boolean reportDropsAsExceptions();
}
