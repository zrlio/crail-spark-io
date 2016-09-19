package org.apache.spark.shuffle.crail

import org.apache.spark.TaskContext
import org.apache.spark.common.Logging
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

/**
 * Created by stu on 22.08.16.
 */
class CrailSparkShuffleSorter extends CrailShuffleSorter with Logging {

  logInfo("rdfs shuffle spark sorter")

  override def sort[K, C](context: TaskContext, keyOrd: Ordering[K], ser : Serializer, inputSerializer: CrailDeserializationStream): Iterator[Product2[K, C]] = {
    val sorter =
      new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = ser)
    val input = inputSerializer.asKeyValueIterator.asInstanceOf[Iterator[Product2[K, C]]]
    sorter.insertAll(input)
    context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
    context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
    context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
    CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
  }
}
