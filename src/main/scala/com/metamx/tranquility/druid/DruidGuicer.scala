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
package com.metamx.tranquility.druid

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.inject.util.Modules
import com.google.inject.{Binder, Guice, Key, Module}
import io.druid.guice.annotations.{Json, Self, Smile}
import io.druid.guice.{DruidSecondaryModule, ExtensionsConfig, FirehoseModule, GuiceInjectors, JsonConfigProvider, LifecycleModule, QueryableModule, ServerModule}
import io.druid.initialization.{DruidModule, Initialization}
import io.druid.server.DruidNode
import scala.collection.JavaConverters._

object DruidGuicer
{
  private[this] val injector = {
    // Adapted from private code in Druid's Initialization class, that we can't call.
    //
    // We don't want to call Initialization.makeInjectorWithModules because we get way too many modules. In particular,
    // the Jetty-related modules seem to cause problems if someone else is doing something similar in the same JVM. We
    // get the scary message, "WARNING: Multiple Servlet injectors detected. This is a warning indicating that you have
    // more than one GuiceFilter running in your web application." So let's try to use as few modules as possible.
    val startupInjector = GuiceInjectors.makeStartupInjector()
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
      new QueryableModule
    ) map toGuiceModule
    val moreModules = Seq(
      new DruidModule
      {
        def getJacksonModules = List.empty[com.fasterxml.jackson.databind.Module].asJava

        def configure(binder: Binder) {
          JsonConfigProvider.bindInstance(
            binder,
            Key.get(classOf[DruidNode], classOf[Self]),
            new DruidNode("dummy", "localhost", -1)
          )
        }
      },
      classOf[DruidSecondaryModule]
    ) map toGuiceModule
    val extensionModules = Initialization.getFromExtensions(
      extensionsConfig,
      classOf[DruidModule]
    ).asScala.toSeq map toGuiceModule
    Guice.createInjector(Modules.`override`(baseModules : _*).`with`(moreModules ++ extensionModules : _*))
  }

  def get[A: ClassManifest]: A = injector.getInstance(classManifest[A].erasure.asInstanceOf[Class[A]])

  def objectMapper = get[ObjectMapper]
}
