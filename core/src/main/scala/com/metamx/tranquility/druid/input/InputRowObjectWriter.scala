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

package com.metamx.tranquility.druid.input

import _root_.io.druid.data.input.InputRow
import _root_.io.druid.data.input.impl.TimestampSpec
import _root_.io.druid.query.aggregation.AggregatorFactory
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.ObjectMapper
import com.metamx.tranquility.druid.DruidSpatialDimension
import com.metamx.tranquility.typeclass.ObjectWriter
import java.io.ByteArrayOutputStream
import scala.collection.JavaConverters._

/**
  * Write InputRows using a Jackson ObjectMapper. Timestamps are written to the timestampColumn from the timestampSpec,
  * but are always written in millis format regardless of the timestampFormat.
  */
class InputRowObjectWriter(
  timestampSpec: TimestampSpec,
  aggregators: IndexedSeq[AggregatorFactory],
  spatialDimensions: Seq[DruidSpatialDimension],
  objectMapper: ObjectMapper,
  override val contentType: String
) extends ObjectWriter[InputRow]
{
  require(timestampSpec.getTimestampFormat == "millis", "Format must be 'millis'")

  private val requiredFields: Set[String] = {
    val metricNames = aggregators.flatMap(_.requiredFields().asScala).distinct
    val spatialDimensionNames = spatialDimensions flatMap { spatial =>
      Option(spatial.schema.getDims).map(_.asScala).getOrElse(Nil) ++
        Option(spatial.schema.getDimName)
    }
    Set() ++ metricNames ++ spatialDimensionNames
  }

  override def asBytes(row: InputRow): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val jg = objectMapper.getFactory.createGenerator(baos)
    writeJson(row, jg)
    jg.close()
    baos.toByteArray
  }

  override def batchAsBytes(rows: TraversableOnce[InputRow]): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val jg = objectMapper.getFactory.createGenerator(baos)
    jg.writeStartArray()
    rows.foreach(writeJson(_, jg))
    jg.writeEndArray()
    jg.close()
    baos.toByteArray
  }

  private def writeJson(row: InputRow, jg: JsonGenerator): Unit = {
    jg.writeStartObject()

    // Write timestamp (as millis-time, regardless of incoming format)
    jg.writeObjectField(timestampSpec.getTimestampColumn, row.getTimestampFromEpoch)

    // Write other fields
    for (fieldName <- requiredFields) {
      jg.writeObjectField(fieldName, row.getRaw(fieldName))
    }

    for (fieldName <- row.getDimensions.asScala) {
      if (!requiredFields.contains(fieldName) && fieldName != timestampSpec.getTimestampColumn) {
        jg.writeObjectField(fieldName, row.getRaw(fieldName))
      }
    }

    jg.writeEndObject()
  }
}
