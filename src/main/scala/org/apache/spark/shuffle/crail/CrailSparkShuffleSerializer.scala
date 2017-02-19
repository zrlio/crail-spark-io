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
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, SerializerInstance}

import scala.reflect.ClassTag

class CrailSparkShuffleSerializer() extends CrailShuffleSerializer {
  override def newCrailSerializer[K,V](dep: ShuffleDependency[K,_,V]): CrailSerializerInstance = {
    new CrailSparkShuffleSerializerInstance(dep.serializer.newInstance())
  }
}


class CrailSparkShuffleSerializerInstance(val serializerInstance: SerializerInstance) extends CrailSerializerInstance {

  override def serializeCrailStream(s: CrailBufferedOutputStream): CrailSerializationStream = {
    new CrailSparkSerializerStream(serializerInstance.serializeStream(s), s)
  }

  override def deserializeCrailStream(s: CrailMultiStream): CrailDeserializationStream = {
    new CrailSparkDeserializerStream(serializerInstance.deserializeStream(s))
  }
}

class CrailSparkSerializerStream(serializerStream: SerializationStream, crailStream: CrailBufferedOutputStream) extends CrailSerializationStream {

  override final def writeObject[T: ClassTag](t: T): SerializationStream = {
    serializerStream.writeObject(t)
  }

  override final def flush(): Unit = {
    serializerStream.flush()
  }

  override final def writeKey[T: ClassTag](key: T): SerializationStream = {
    serializerStream.writeKey(key)
  }
  /** Writes the object representing the value of a key-value pair. */
  override final def writeValue[T: ClassTag](value: T): SerializationStream = {
    serializerStream.writeValue(value)
  }

  override final def close(): Unit = {
    serializerStream.close()
  }

  override final def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
    serializerStream.writeAll(iter)
  }

  def dataWritten() : Long = {
    crailStream.position()
  }
}

class CrailSparkDeserializerStream(deserializerStream: DeserializationStream) extends CrailDeserializationStream {

  override final def readObject[T: ClassTag](): T = {
    deserializerStream.readObject()
  }

  /* for key we return the address */
  override final def readKey[T: ClassTag](): T = {
    deserializerStream.readKey()
  }

  override final def readValue[T: ClassTag](): T = {
    deserializerStream.readValue()
  }

  override final def close(): Unit = {
    deserializerStream.close()
  }

  override def asIterator : scala.Iterator[scala.Any] = {
    return deserializerStream.asIterator
  }

  override def asKeyValueIterator : scala.Iterator[scala.Tuple2[scala.Any, scala.Any]] = {
    return deserializerStream.asKeyValueIterator
  }

  override def read(buf: ByteBuffer): Int = {
    0
  }

  override def available(): Int = {
    0
  }
}
