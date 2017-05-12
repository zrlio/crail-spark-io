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

import com.ibm.crail.{CrailBufferedInputStream, CrailBufferedOutputStream}
import org.apache.spark.ShuffleDependency

import scala.reflect.ClassTag

class CrailSparkSerializer() extends CrailSerializer {
  override def newCrailSerializer(defaultSerializer: Serializer): CrailSerializerInstance = {
    new CrailSparkSerializerInstance(defaultSerializer)
  }
}

class CrailSparkSerializerInstance(val defaultSerializer: Serializer) extends CrailSerializerInstance {



  override def serializeCrailStream(s: CrailBufferedOutputStream): CrailSerializationStream = {
    new CrailSparkSerializationStream(defaultSerializer, s)
  }

  override def deserializeCrailStream(s: CrailBufferedInputStream): CrailDeserializationStream = {
    new CrailSparkDeserializationStream(defaultSerializer, s)
  }
}

object CrailSparkSerializerInstance {
  val byteArrayMark:Int = 1
  val otherMark:Int = 2
}

class CrailSparkSerializationStream(defaultSerializer: Serializer, crailStream: CrailBufferedOutputStream) extends CrailSerializationStream {

//  lazy val serializerStream = defaultSerializer.newInstance().serializeStream(crailStream)
  var serializerStream : SerializationStream = null

  final def writeBroadcast[T: ClassTag](value: T): Unit = {
    value match {
      case arr: Array[Byte] => {
        /* write the mark */
        crailStream.writeInt(CrailSparkSerializerInstance.byteArrayMark)
        /* write the size */
        crailStream.writeInt(arr.length)
        /* write the data structure */
        crailStream.write(arr)
      }
      case o: Any => {
//        val serializerStream = defaultSerializer.newInstance().serializeStream(crailStream)
        crailStream.writeInt(CrailSparkSerializerInstance.otherMark)
        getDefaultSerializationStream().writeObject(value)
      }
    }
  }

  override final def writeObject[T: ClassTag](value: T): SerializationStream = {
    getDefaultSerializationStream().writeObject(value)
  }

  override final def flush(): Unit = {
    getDefaultSerializationStream().flush()
  }

  override final def writeKey[T: ClassTag](key: T): SerializationStream = {
    getDefaultSerializationStream().writeKey(key)
  }
  /** Writes the object representing the value of a key-value pair. */
  override final def writeValue[T: ClassTag](value: T): SerializationStream = {
    getDefaultSerializationStream().writeValue(value)
  }

  override final def close(): Unit = {
    if (serializerStream != null){
      serializerStream.flush()
      serializerStream.close()
    } else {
      crailStream.close()
    }
  }

  override final def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
    getDefaultSerializationStream().writeAll(iter)
  }

  def dataWritten() : Long = {
    crailStream.position()
  }

  private def getDefaultSerializationStream() : SerializationStream = {
    if (serializerStream == null){
      serializerStream = defaultSerializer.newInstance().serializeStream(crailStream)
    }
    serializerStream
  }
}

class CrailSparkDeserializationStream(defaultSerializer: Serializer, crailStream: CrailBufferedInputStream) extends CrailDeserializationStream {

//  lazy val deserializerStream = defaultSerializer.newInstance().deserializeStream(crailStream)
  var deserializerStream : DeserializationStream = null

  final def readBroadcast(): Any = {
    crailStream.readInt() match {
      case CrailSparkSerializerInstance.byteArrayMark => {
        /* get the size of the array */
        val size = crailStream.readInt()
        /* allocate the array and read it in */
        val byteArray = new Array[Byte](size)
        crailStream.read(byteArray)
        byteArray
      }
      case CrailSparkSerializerInstance.otherMark => {
        getDefaultSerializationStream().readObject()
      }
    }
  }

  override final def readObject[T: ClassTag](): T = {
    getDefaultSerializationStream().readObject()
  }

  /* for key we return the address */
  override final def readKey[T: ClassTag](): T = {
    getDefaultSerializationStream().readKey()
  }

  override final def readValue[T: ClassTag](): T = {
    getDefaultSerializationStream().readValue()
  }

  override final def close(): Unit = {
    if (deserializerStream != null){
      deserializerStream.close()
    } else {
      crailStream.close()
    }
  }

  override def asIterator : scala.Iterator[scala.Any] = {
    getDefaultSerializationStream().asIterator
  }

  override def asKeyValueIterator : scala.Iterator[scala.Tuple2[scala.Any, scala.Any]] = {
    getDefaultSerializationStream().asKeyValueIterator
  }

  override def read(buf: ByteBuffer): Int = {
    0
  }

  override def available(): Int = {
    0
  }

  private def getDefaultSerializationStream() : DeserializationStream = {
    if (deserializerStream == null){
      deserializerStream = defaultSerializer.newInstance().deserializeStream(crailStream)
    }
    deserializerStream
  }
}
