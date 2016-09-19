package org.apache.spark.shuffle.crail

import java.io.{EOFException}
import java.nio.ByteBuffer
import com.ibm.crail.{CrailMultiStream, CrailInputStream, CrailBufferedOutputStream}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.serializer._
import org.apache.spark.util.NextIterator
import scala.reflect.ClassTag

/**
 * Created by stu on 08.09.16.
 */
trait CrailShuffleSerializer {
  def newCrailSerializer(): CrailSerializerInstance
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
