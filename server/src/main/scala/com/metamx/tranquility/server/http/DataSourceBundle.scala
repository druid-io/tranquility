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

package com.metamx.tranquility.server.http

import com.metamx.tranquility.druid.input.ThreadLocalInputRowParser
import com.metamx.tranquility.tranquilizer.Tranquilizer
import io.druid.data.input.InputRow
import io.druid.data.input.impl.MapInputRowParser
import io.druid.data.input.impl.ParseSpec
import io.druid.data.input.impl.StringInputRowParser

class DataSourceBundle(
  val tranquilizer: Tranquilizer[InputRow],
  val parseSpec: ParseSpec
)
{
  private val threadLocalStringParser = new ThreadLocalInputRowParser(() => new StringInputRowParser(parseSpec, null))
  private val threadLocalMapParser    = new ThreadLocalInputRowParser(() => new MapInputRowParser(parseSpec))

  def stringParser: StringInputRowParser = threadLocalStringParser.get()

  def mapParser: MapInputRowParser = threadLocalMapParser.get()
}
