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
package com.metamx.tranquility.druid

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.inject.Binder
import com.google.inject.Guice
import com.google.inject.Key
import com.google.inject.Module
import com.google.inject.util.Modules
import io.druid.guice.ConfigModule
import io.druid.guice.DruidGuiceExtensions
import io.druid.guice.DruidSecondaryModule
import io.druid.guice.ExtensionsConfig
import io.druid.guice.FirehoseModule
import io.druid.guice.JsonConfigProvider
import io.druid.guice.LifecycleModule
import io.druid.guice.QueryableModule
import io.druid.guice.ServerModule
import io.druid.guice.annotations.Json
import io.druid.guice.annotations.Self
import io.druid.guice.annotations.Smile
import io.druid.initialization.DruidModule
import io.druid.initialization.Initialization
import io.druid.jackson.JacksonModule
import io.druid.server.DruidNode
import java.util.Properties
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.classTag

class DruidGuicer(props: Properties)
{
  private[this] val injector = {
    // Adapted from private code in Druid's Initialization class, that we can't call.
    //
    // We don't want to call Initialization.makeInjectorWithModules because we get way too many modules. In particular,
    // the Jetty-related modules seem to cause problems if someone else is doing something similar in the same JVM. We
    // get the scary message, "WARNING: Multiple Servlet injectors detected. This is a warning indicating that you have
    // more than one GuiceFilter running in your web application." So let's try to use as few modules as possible.
    val extensionsConfigModule = new Module
    {
      override def configure(binder: Binder): Unit = {
        binder.bind(classOf[DruidSecondaryModule])
        JsonConfigProvider.bind(binder, "druid.extensions", classOf[ExtensionsConfig])
      }
    }
    val startupInjector = Guice.createInjector(
      new DruidGuiceExtensions,
      new JacksonModule,
      new Module
      {
        override def configure(binder: Binder): Unit = {
          val theProps = new Properties
          theProps.putAll(props)
          theProps.putAll(System.getProperties)
          binder.bind(classOf[Properties]).toInstance(theProps)
        }
      },
      new ConfigModule,
      extensionsConfigModule
    )
    def registerWithJackson(m: DruidModule) {
      m.getJacksonModules.asScala foreach {
        jacksonModule =>
          startupInjector.getInstance(Key.get(classOf[ObjectMapper], classOf[Json])).registerModule(jacksonModule)
          startupInjector.getInstance(Key.get(classOf[ObjectMapper], classOf[Smile])).registerModule(jacksonModule)
      }
    }
    def toGuiceModule(o: Any): Module = o match {
      case m: DruidModule =>
        startupInjector.injectMembers(m)
        registerWithJackson(m)
        m
      case m: Module =>
        startupInjector.injectMembers(m)
        m
      case clazz: Class[_] if classOf[DruidModule].isAssignableFrom(clazz) =>
        val m = startupInjector.getInstance(clazz.asInstanceOf[Class[_ <: DruidModule]])
        registerWithJackson(m)
        m
      case clazz: Class[_] if classOf[Module].isAssignableFrom(clazz) =>
        startupInjector.getInstance(clazz.asInstanceOf[Class[_ <: Module]])
    }
    val extensionsConfig = startupInjector.getInstance(classOf[ExtensionsConfig])
    val baseModules = Seq(
      new LifecycleModule,
      new FirehoseModule,
      new ServerModule,
      new QueryableModule,
      extensionsConfigModule
    ) map toGuiceModule
    val moreModules = Seq(
      new DruidModule
      {
        def getJacksonModules = List.empty[com.fasterxml.jackson.databind.Module].asJava

        def configure(binder: Binder) {
          JsonConfigProvider.bindInstance(
            binder,
            Key.get(classOf[DruidNode], classOf[Self]),
            new DruidNode("dummy", null, null)
          )
        }
      },
      classOf[DruidSecondaryModule]
    ) map toGuiceModule
    val extensionModules = Initialization.getFromExtensions(
      extensionsConfig,
      classOf[DruidModule]
    ).asScala.toSeq map toGuiceModule
    Guice.createInjector(Modules.`override`(baseModules: _*).`with`(moreModules ++ extensionModules: _*))
  }

  def get[A: ClassTag]: A = injector.getInstance(classTag[A].runtimeClass.asInstanceOf[Class[A]])

  def objectMapper = get[ObjectMapper]
}

object DruidGuicer
{
  val Default = new DruidGuicer(new Properties())
}
