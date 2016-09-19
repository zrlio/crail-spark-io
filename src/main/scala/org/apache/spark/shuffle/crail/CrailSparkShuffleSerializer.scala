package org.apache.spark.shuffle.crail


import java.io._
import java.nio.ByteBuffer
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import com.ibm.crail.{CrailMultiStream, CrailBufferedOutputStream}
import scala.reflect.ClassTag

///**
// * Created by stu on 13.09.16.
// */
//class CrailSparkShuffleSerializer {
//
//}


class CrailSparkShuffleSerializer(val serializer: Serializer) extends Serializer with CrailShuffleSerializer {
  override final def newInstance(): SerializerInstance = {
    new CrailSparkShuffleSerializerInstance(serializer.newInstance())
  }
  override lazy val supportsRelocationOfSerializedObjects: Boolean = true

  override def newCrailSerializer(): CrailSerializerInstance = {
    new CrailSparkShuffleSerializerInstance(serializer.newInstance())
  }
}


class CrailSparkShuffleSerializerInstance(val serializerInstance: SerializerInstance) extends SerializerInstance with CrailSerializerInstance {

  override final def serialize[T: ClassTag](t: T): ByteBuffer = {
    serializerInstance.serialize(t)
  }

  override final def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    serializerInstance.deserialize(bytes)
  }

  override final def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
    serializerInstance.deserialize(bytes, loader)
  }

  override final def serializeStream(s: OutputStream): SerializationStream = {
    serializerInstance.serializeStream(s)
  }

  /* this is the one we are interested in */
  override final def deserializeStream(s: InputStream): DeserializationStream = {
    serializerInstance.deserializeStream(s)
  }

  override def serializeCrailStream(s: CrailBufferedOutputStream): CrailSerializationStream = {
    new CrailSparkSerializerStream(serializerInstance.serializeStream(s))
  }

  override def deserializeCrailStream(s: CrailMultiStream): CrailDeserializationStream = {
    new CrailSparkDeserializerStream(serializerInstance.deserializeStream(s))
  }
}

class CrailSparkSerializerStream(serializerStream: SerializationStream) extends CrailSerializationStream {

  override final def writeObject[T: ClassTag](t: T): SerializationStream = {
    serializerStream.writeObject(t)
  }

  override final def flush(): Unit = {
    serializerStream.flush()
  }

  override final def writeKey[T: ClassTag](key: T): SerializationStream = writeObject(key)
  /** Writes the object representing the value of a key-value pair. */
  override final def writeValue[T: ClassTag](value: T): SerializationStream = writeObject(value)

  override final def close(): Unit = {
    serializerStream.close()
  }

  override final def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
    serializerStream.writeAll(iter)
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

  override def getFlatBuffer(): ByteBuffer = {
    null
  }

  override def valueSize(): Int = 0

  override def keySize(): Int = 0

  override def numElements(): Int = 0
}
