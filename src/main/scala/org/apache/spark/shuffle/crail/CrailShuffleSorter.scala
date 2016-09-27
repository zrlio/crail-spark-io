package org.apache.spark.shuffle.crail

import org.apache.spark.TaskContext
import org.apache.spark.serializer.Serializer

/**
 * Created by stu on 22.08.16.
 */
trait CrailShuffleSorter {
  def sort[K, C](context: TaskContext, keyOrd: Ordering[K], ser : Serializer, crailDeserializationStream: CrailDeserializationStream): Iterator[Product2[K, C]]
}
