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

import org.apache.spark._
import org.apache.spark.common._
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleReader}
import org.apache.spark.storage._


class CrailShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    crailSerializer: CrailShuffleSerializer,
    crailSorter: CrailShuffleSorter,
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
  extends ShuffleReader[K, C] with Logging
{
  require(endPartition == startPartition + 1,
    "Hash shuffle currently only supports fetching one partition")

  private val dep = handle.dependency
  private val serializerInstance = crailSerializer.newCrailSerializer(dep)

  /** Read the combined key-values for this reduce task */
  override def read(): Iterator[Product2[K, C]] = {
    val multiStream = CrailStore.get.getMultiStream(handle.shuffleId, startPartition, handle.numMaps)
    val deserializationStream = serializerInstance.deserializeCrailStream(multiStream)
    dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        new CrailInputCloser(multiStream, crailSorter.sort(context, keyOrd, dep.serializer, deserializationStream))
      case None =>
        new CrailInputCloser(multiStream, deserializationStream.asKeyValueIterator.asInstanceOf[Iterator[Product2[K, C]]])
      }
  }
}

