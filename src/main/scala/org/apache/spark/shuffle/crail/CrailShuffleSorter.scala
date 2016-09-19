package org.apache.spark.shuffle.crail

import org.apache.spark.InternalAccumulator.input
import org.apache.spark.serializer.Serializer
import org.apache.spark.{TaskContext, SecurityManager, SparkConf}

/**
 * Created by stu on 22.08.16.
 */
trait CrailShuffleSorter {
  def sort[K, C](context: TaskContext, keyOrd: Ordering[K], ser : Serializer, crailDeserializationStream: CrailDeserializationStream): Iterator[Product2[K, C]]
}
