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

  lazy val serializerStream = defaultSerializer.newInstance().serializeStream(crailStream)

  override final def writeObject[T: ClassTag](value: T): SerializationStream = {
    value match {
      case arr: Array[Byte] => {
        /* write the mark */
        crailStream.writeInt(CrailSparkSerializerInstance.byteArrayMark)
        /* write the size */
        crailStream.writeInt(arr.length)
        /* write the data structure */
        crailStream.write(arr)
        /* close will purge as well */
        crailStream.close()
      }
      case o: Any => {
        crailStream.writeInt(CrailSparkSerializerInstance.otherMark)
        serializerStream.writeObject(value)
      }
    }
    this
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

class CrailSparkDeserializationStream(defaultSerializer: Serializer, crailStream: CrailBufferedInputStream) extends CrailDeserializationStream {

  lazy val deserializerStream = defaultSerializer.newInstance().deserializeStream(crailStream)

  override final def readObject[T: ClassTag](): T = {
    crailStream.readInt() match {
      case CrailSparkSerializerInstance.byteArrayMark => {
        /* get the size of the array */
        val size = crailStream.readInt()
        /* allocate the array and read it in */
        val byteArray = new Array[Byte](size)
        crailStream.read(byteArray)
        crailStream.close()
        Some(byteArray)
      }
      case CrailSparkSerializerInstance.otherMark => {
        deserializerStream.readObject()
      }
    }



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
