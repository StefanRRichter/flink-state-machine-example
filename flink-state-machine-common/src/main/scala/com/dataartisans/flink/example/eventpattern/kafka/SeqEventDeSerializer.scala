/*
 * Copyright 2015 Data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flink.example.eventpattern.kafka

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.dataartisans.flink.example.eventpattern.{Event, SeqEvent}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.memory.{DataInputViewStreamWrapper, DataOutputViewStreamWrapper}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.types.StringValue

/**
 * A serializer / Deserializer for converting [[com.dataartisans.flink.example.eventpattern.Event]]
 * objects from/to byte sequences for Kafka.
 */
class SeqEventDeSerializer extends DeserializationSchema[SeqEvent] with SerializationSchema[SeqEvent] {
  
  override def deserialize(bytes: Array[Byte]): SeqEvent = {
    val bais = new ByteArrayInputStream(bytes)
    val inputView = new DataInputViewStreamWrapper(bais)
    val address = StringValue.readString(inputView)
    val value: Long = inputView.readLong()
    SeqEvent(address, value)
  }

  override def serialize(t: SeqEvent): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val outputView = new DataOutputViewStreamWrapper(baos)
    StringValue.writeString(t.sourceAddress, outputView)
    outputView.writeLong(t.value)
    outputView.flush()
    baos.toByteArray
  }

  override def isEndOfStream(t: SeqEvent): Boolean = false

  override def getProducedType: TypeInformation[SeqEvent] = {
    createTypeInformation[SeqEvent]
  }
}
