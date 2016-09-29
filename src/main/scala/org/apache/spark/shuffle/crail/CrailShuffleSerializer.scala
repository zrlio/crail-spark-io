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

package org.apache.spark.shuffle.crail

import java.nio.ByteBuffer

import com.ibm.crail.{CrailBufferedOutputStream, CrailMultiStream}
import org.apache.spark.ShuffleDependency
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.serializer._

/**
 * Created by stu on 08.09.16.
 */
trait CrailShuffleSerializer {
  def newCrailSerializer[K, V](dep: ShuffleDependency[K,_,V]): CrailSerializerInstance
}

trait CrailSerializerInstance {
  def serializeCrailStream(s: CrailBufferedOutputStream): CrailSerializationStream

  def deserializeCrailStream(s: CrailMultiStream): CrailDeserializationStream
}

@DeveloperApi
abstract class CrailSerializationStream extends SerializationStream {
}

@DeveloperApi
abstract class CrailDeserializationStream extends DeserializationStream {
  def getFlatBuffer() : ByteBuffer
  def keySize(): Int
  def valueSize(): Int
  def numElements(): Int
}
