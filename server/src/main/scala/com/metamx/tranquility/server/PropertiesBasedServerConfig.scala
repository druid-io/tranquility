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

package com.metamx.tranquility.server

import java.security.KeyStore
import java.{util => ju}
import javax.net.ssl.KeyManagerFactory

import com.metamx.tranquility.config.PropertiesBasedConfig
import org.joda.time.Period
import org.skife.config.Config

abstract class PropertiesBasedServerConfig
  extends PropertiesBasedConfig(Set("http.port", "http.threads", "http.idleTimeout"))
{
  @Config(Array("http.port"))
  def httpPort: Int = 8200

  @Config(Array("http.port.enable"))
  def httpPortEnable: Boolean = true

  @Config(Array("https.port"))
  def httpsPort: Int = 8400

  @Config(Array("https.port.enable"))
  def httpsPortEnable: Boolean = false

  @Config(Array("https.keyStorePath"))
  def httpsKeyStorePath: String = ""

  @Config(Array("https.keyStoreType"))
  def httpsKeyStoreType: String = KeyStore.getDefaultType

  @Config(Array("https.keyStorePassword"))
  def httpsKeyStorePassword: String = ""

  @Config(Array("https.certAlias"))
  def httpsCertAlias: String = ""

  @Config(Array("https.keyManagerFactoryAlgorithm"))
  def httpsKeyManagerFactoryAlgorithm: String = KeyManagerFactory.getDefaultAlgorithm

  @Config(Array("https.keyManagerPassword"))
  def httpsKeyManagerPassword: String = ""

  @Config(Array("https.includeCipherSuites"))
  def httpsIncludeCipherSuites: ju.List[String] = null

  @Config(Array("https.excludeCipherSuites"))
  def httpsExcludeCipherSuites: ju.List[String] = null

  @Config(Array("https.includeProtocols"))
  def httpsIncludeProtocols: ju.List[String] = null

  @Config(Array("https.excludeProtocols"))
  def httpsExcludeProtocols: ju.List[String] = null

  @Config(Array("http.threads"))
  def httpThreads: Int = 40

  @Config(Array("http.idleTimeout"))
  def httpIdleTimeout: Period = new Period("PT5M")
}
