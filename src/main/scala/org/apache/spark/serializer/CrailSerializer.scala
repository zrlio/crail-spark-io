/*
 * Spark-IO: Fast storage and network I/O for Spark
 *
 * Author: Patrick Stuedi <stu@zurich.ibm.com>
 *
 * Copyright (C) 2016, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.serializer

import java.nio.ByteBuffer

import org.apache.crail.{CrailBufferedInputStream, CrailBufferedOutputStream}
import org.apache.spark.annotation.DeveloperApi
import scala.reflect.ClassTag

/**
 * Created by stu on 08.09.16.
 */
trait CrailSerializer {
  def newCrailSerializer(defaultSerializer: Serializer): CrailSerializerInstance
}

trait CrailSerializerInstance {
  def serializeCrailStream(s: CrailBufferedOutputStream): CrailSerializationStream

  def deserializeCrailStream(s: CrailBufferedInputStream): CrailDeserializationStream
}

@DeveloperApi
abstract class CrailSerializationStream extends SerializationStream {
  def writeBroadcast[T: ClassTag](value: T): Unit
}

@DeveloperApi
abstract class CrailDeserializationStream extends DeserializationStream {
  def readBroadcast(): Any
  def read(buf: ByteBuffer) : Int
  def available() : Int
}
