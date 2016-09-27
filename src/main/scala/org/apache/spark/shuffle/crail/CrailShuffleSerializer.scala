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
