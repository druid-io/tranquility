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

import com.fasterxml.jackson.core.JsonGenerator
import com.google.common.base.Charsets
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.collection.implicits._
import com.metamx.common.scala.untyped._
import com.metamx.tranquility.beam.Beam
import com.metamx.tranquility.beam.MemoryBeam
import com.metamx.tranquility.server.ServerTestUtil.withTester
import com.metamx.tranquility.server.TranquilityServletTest._
import com.metamx.tranquility.test.common.FailableBeam
import com.metamx.tranquility.typeclass.JsonWriter
import com.twitter.util.Await
import com.twitter.util.Future
import io.druid.data.input.InputRow
import io.druid.data.input.MapBasedInputRow
import io.druid.data.input.impl.CSVParseSpec
import io.druid.data.input.impl.DimensionsSpec
import io.druid.data.input.impl.TimestampSpec
import org.scalatest.FunSuite
import org.scalatest.ShouldMatchers
import scala.collection.JavaConverters._

class TranquilityServletTest extends FunSuite with ShouldMatchers
{
  test("Hello world") {
    val events = withBeams { beams =>
      withTester(beams) { tester =>
        tester.get("/") {
          tester.status should be(200)
          tester.header("Content-Type") should startWith("text/plain;")
        }
      }
    }

    events should be(Map.empty)
  }

  test("/v1/post, application/json, json array") {
    val events = withBeams { beams =>
      withTester(beams) { tester =>
        val body = Jackson.bytes(
          Seq(
            Dict("dataSource" -> "foo", "n" -> 1),
            Dict("dataSource" -> "foo", "n" -> 2),
            Dict("feed" -> "bar", "n" -> 3),
            Dict("dataSource" -> "bar", "n" -> 4, FailableBeam.ActionKey -> "__drop__"),
            Dict("dataSource" -> "bar", "n" -> 5)
          )
        )

        tester.post("/v1/post", body, JsonHeaders) {
          tester.status should be(200)
          tester.header("Content-Type") should startWith("application/json;")
          val response = Jackson.parse[Dict](tester.bodyBytes)
          response should be(
            Dict(
              "result" -> Dict(
                "received" -> 5,
                "sent" -> 4
              )
            )
          )
        }
      }
    }

    events should be(
      Map(
        "foo" -> Seq(
          Dict("dataSource" -> "foo", "n" -> 1),
          Dict("dataSource" -> "foo", "n" -> 2)
        ),
        "bar" -> Seq(
          Dict("feed" -> "bar", "n" -> 3),
          Dict("dataSource" -> "bar", "n" -> 5)
        )
      )
    )
  }

  test("/v1/post, application/json, errors") {
    withBeams { beams =>
      withTester(beams) { tester =>
        val body = Jackson.bytes(
          Seq(
            Dict("dataSource" -> "foo", "n" -> 1),
            Dict("dataSource" -> "foo", "n" -> 2),
            Dict("feed" -> "bar", "n" -> 3),
            Dict("dataSource" -> "bar", "n" -> 4, FailableBeam.ActionKey -> "__fail__"),
            Dict("dataSource" -> "bar", "n" -> 5)
          )
        )

        tester.post("/v1/post", body, JsonHeaders) {
          tester.status should be(500)
          tester.header("Content-Type") should startWith("text/plain;")
          tester.body should be("Server error\n")
        }
      }
    }
  }

  test("/v1/post, application/json, unrecognized dataSource") {
    withBeams { beams =>
      withTester(beams) { tester =>
        val body = Jackson.bytes(
          Seq(
            Dict("dataSource" -> "baz", "n" -> 1)
          )
        )

        tester.post("/v1/post", body, JsonHeaders) {
          tester.status should be(400)
          tester.header("Content-Type") should startWith("text/plain;")
          tester.body should be("No definition for dataSource 'baz'\n")
        }
      }
    }
  }

  test("/v1/post, application/json, newline delimited json") {
    val events = withBeams { beams =>
      withTester(beams) { tester =>
        val body = Seq(
          Dict("dataSource" -> "foo", "n" -> 1),
          Dict("dataSource" -> "foo", "n" -> 2),
          Dict("feed" -> "bar", "n" -> 3),
          Dict("dataSource" -> "bar", "n" -> 4, FailableBeam.ActionKey -> "__drop__"),
          Dict("dataSource" -> "bar", "n" -> 5)
        ).map(Jackson.generate(_)).mkString("\n").getBytes(Charsets.UTF_8)

        tester.post("/v1/post", body, JsonHeaders) {
          tester.status should be(200)
          tester.header("Content-Type") should startWith("application/json;")
          val response = Jackson.parse[Dict](tester.bodyBytes)
          response should be(
            Dict(
              "result" -> Dict(
                "received" -> 5,
                "sent" -> 4
              )
            )
          )
        }
      }
    }

    events should be(
      Map(
        "foo" -> Seq(
          Dict("dataSource" -> "foo", "n" -> 1),
          Dict("dataSource" -> "foo", "n" -> 2)
        ),
        "bar" -> Seq(
          Dict("feed" -> "bar", "n" -> 3),
          Dict("dataSource" -> "bar", "n" -> 5)
        )
      )
    )
  }

  test("/v1/post/dataSource, application/json, json array") {
    val events = withBeams { beams =>
      withTester(beams) { tester =>
        val body = Jackson.bytes(
          Seq(
            Dict("dataSource" -> "foo", "n" -> 1),
            Dict("dataSource" -> "foo", "n" -> 2),
            Dict("feed" -> "bar", "n" -> 3),
            Dict("dataSource" -> "bar", "n" -> 4, FailableBeam.ActionKey -> "__drop__"),
            Dict("dataSource" -> "bar", "n" -> 5)
          )
        )

        tester.post("/v1/post/foo", body, JsonHeaders) {
          tester.status should be(200)
          tester.header("Content-Type") should startWith("application/json;")
          val response = Jackson.parse[Dict](tester.bodyBytes)
          response should be(
            Dict(
              "result" -> Dict(
                "received" -> 5,
                "sent" -> 4
              )
            )
          )
        }
      }
    }

    events should be(
      Map(
        "foo" -> Seq(
          Dict("dataSource" -> "foo", "n" -> 1),
          Dict("dataSource" -> "foo", "n" -> 2),
          Dict("feed" -> "bar", "n" -> 3),
          Dict("dataSource" -> "bar", "n" -> 5)
        )
      )
    )
  }

  test("/v1/post/dataSource, text/plain, csv") {
    val events = withBeams { beams =>
      val parseSpec = new CSVParseSpec(
        new TimestampSpec("ts", "posix", null),
        new DimensionsSpec(Seq("ts", "dataSource", "n").asJava, null, null),
        null,
        Seq("ts", "dataSource", "n", FailableBeam.ActionKey).asJava
      )
      withTester(beams, Map("foo" -> parseSpec)) { tester =>
        val body = Seq(
          "1,foo,1,x",
          "1,foo,2,x",
          "1,bar,3,x",
          "1,bar,4,__drop__",
          "1,bar,5,x"
        ).mkString("\n").getBytes(Charsets.UTF_8)

        tester.post("/v1/post/foo", body, TextHeaders) {
          tester.status should be(200)
          tester.header("Content-Type") should startWith("application/json;")
          val response = Jackson.parse[Dict](tester.bodyBytes)
          response should be(
            Dict(
              "result" -> Dict(
                "received" -> 5,
                "sent" -> 4
              )
            )
          )
        }
      }
    }

    events should be(
      Map(
        "foo" -> Seq(
          Dict("ts" -> "1", "dataSource" -> "foo", "n" -> "1", FailableBeam.ActionKey -> "x"),
          Dict("ts" -> "1", "dataSource" -> "foo", "n" -> "2", FailableBeam.ActionKey -> "x"),
          Dict("ts" -> "1", "dataSource" -> "bar", "n" -> "3", FailableBeam.ActionKey -> "x"),
          Dict("ts" -> "1", "dataSource" -> "bar", "n" -> "5", FailableBeam.ActionKey -> "x")
        )
      )
    )
  }

  test("/v1/post?async=true, application/json, json array") {
    val events = withBeams { beams =>
      withTester(beams) { tester =>
        val body = Jackson.bytes(
          Seq(
            Dict("dataSource" -> "foo", "n" -> 1),
            Dict("dataSource" -> "foo", "n" -> 2),
            Dict("feed" -> "bar", "n" -> 3),
            Dict("dataSource" -> "bar", "n" -> 4, FailableBeam.ActionKey -> "__drop__"),
            Dict("dataSource" -> "bar", "n" -> 5)
          )
        )

        tester.post("/v1/post?async=true", body, JsonHeaders) {
          tester.status should be(200)
          tester.header("Content-Type") should startWith("application/json;")
          val response = Jackson.parse[Dict](tester.bodyBytes)
          response should be(
            Dict(
              "result" -> Dict(
                "received" -> 5,
                "sent" -> 0
              )
            )
          )
        }
      }
    }

    events should be(
      Map(
        "foo" -> Seq(
          Dict("dataSource" -> "foo", "n" -> 1),
          Dict("dataSource" -> "foo", "n" -> 2)
        ),
        "bar" -> Seq(
          Dict("feed" -> "bar", "n" -> 3),
          Dict("dataSource" -> "bar", "n" -> 5)
        )
      )
    )
  }

  test("/v1/post/dataSource?async=true, application/json, json array") {
    val events = withBeams { beams =>
      withTester(beams) { tester =>
        val body = Jackson.bytes(
          Seq(
            Dict("dataSource" -> "foo", "n" -> 1),
            Dict("dataSource" -> "foo", "n" -> 2),
            Dict("feed" -> "bar", "n" -> 3),
            Dict("dataSource" -> "bar", "n" -> 4, FailableBeam.ActionKey -> "__drop__"),
            Dict("dataSource" -> "bar", "n" -> 5)
          )
        )

        tester.post("/v1/post/foo?async=true", body, JsonHeaders) {
          tester.status should be(200)
          tester.header("Content-Type") should startWith("application/json;")
          val response = Jackson.parse[Dict](tester.bodyBytes)
          response should be(
            Dict(
              "result" -> Dict(
                "received" -> 5,
                "sent" -> 0
              )
            )
          )
        }
      }
    }

    events should be(
      Map(
        "foo" -> Seq(
          Dict("dataSource" -> "foo", "n" -> 1),
          Dict("dataSource" -> "foo", "n" -> 2),
          Dict("feed" -> "bar", "n" -> 3),
          Dict("dataSource" -> "bar", "n" -> 5)
        )
      )
    )
  }
}

object TranquilityServletTest
{
  private val JsonHeaders = Map("Content-Type" -> "application/json")
  private val TextHeaders = Map("Content-Type" -> "text/plain")

  private def withBeams(f: Map[String, Beam[InputRow]] => Unit): Map[String, Seq[Dict]] =
  {
    val memoryBeams: Map[String, MemoryBeam[InputRow]] = (for (dataSource <- Seq("foo", "bar")) yield {
      val beam = new MemoryBeam[InputRow](
        dataSource,
        new JsonWriter[InputRow]
        {
          override protected def viaJsonGenerator(d: InputRow, jg: JsonGenerator): Unit = {
            Jackson.generate(d.asInstanceOf[MapBasedInputRow].getEvent, jg)
          }
        }
      )
      (dataSource, beam)
    }).toMap

    val beams: Map[String, Beam[InputRow]] = memoryBeams strictMapValues { memoryBeam =>
      FailableBeam.forInputRows(memoryBeam)
    }

    MemoryBeam.clear()

    try {
      f(beams)
    }
    finally {
      Await.result(Future.collect(beams.map(_._2.close()).toSeq))
    }

    memoryBeams strictMapValues { memoryBeam =>
      MemoryBeam.get().getOrElse(memoryBeam.key, Nil)
    } filter { case (dataSource, events) =>
      events.nonEmpty
    }
  }
}
