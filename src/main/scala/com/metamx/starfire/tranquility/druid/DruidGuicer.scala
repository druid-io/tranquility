package com.metamx.starfire.tranquility.druid

import com.fasterxml.jackson.databind.{Module, ObjectMapper}
import com.google.inject.{Binder, Key}
import io.druid.guice.JsonConfigProvider
import io.druid.guice.annotations.Self
import io.druid.initialization.{DruidModule, Initialization}
import io.druid.server.DruidNode
import java.{util => ju}
import scala.collection.JavaConverters._

object DruidGuicer
{
  private[this] val injector = {
    val startupInjector = Initialization.makeStartupInjector
    Initialization.makeInjectorWithModules(
      startupInjector,
      Seq[AnyRef](
        new DruidModule
        {
          def getJacksonModules = new ju.ArrayList[Module]()

          def configure(binder: Binder) {
            JsonConfigProvider.bindInstance(
              binder,
              Key.get(classOf[DruidNode], classOf[Self]),
              new DruidNode("starfire", "localhost", -1)
            )
          }
        }
      ).asJava
    )
  }

  def get[A: ClassManifest]: A = injector.getInstance(classManifest[A].erasure.asInstanceOf[Class[A]])

  def objectMapper = get[ObjectMapper]
}
