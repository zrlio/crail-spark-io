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

package org.apache.spark.broadcast

import java.io._

import org.apache.spark.executor.DataReadMethod
import org.apache.spark.storage._
import org.apache.spark.{SparkEnv, SparkException}

import scala.reflect.ClassTag
import scala.util.control.NonFatal


private[spark] class CrailBroadcast[T: ClassTag](obj: T, id: Long)
  extends Broadcast[T](id) with Serializable {
  
  @transient private lazy val _value: T = readBroadcastBlock()
  
  private val broadcastId = BroadcastBlockId(id)
  
  writeBlocks(obj)
  
  override protected def getValue() = {
    _value
  }
  
  override protected def doUnpersist(blocking: Boolean) {
  }  
  
  override protected def doDestroy(blocking: Boolean) {
  }
  
  private def writeBlocks(value: T) {
    logInfo("RdfsBroadcast, writeBlocks")
    CrailStore.get.putValues(broadcastId, Iterator(value))
    SparkEnv.get.blockManager.master.updateBlockInfo(SparkEnv.get.blockManager.blockManagerId, broadcastId, StorageLevel.OFF_HEAP, 32, 32)
  }
  
  private def readBroadcastBlock(): T = Utils.tryOrIOException {
    CrailBroadcast.synchronized {
      CrailStore.get.getValues(broadcastId).map(new BlockResult(_, DataReadMethod.Memory, 32)).map(_.data.next()) match {
        case Some(x) =>
          val obj = x.asInstanceOf[T]
          SparkEnv.get.blockManager.putSingle(
            broadcastId, obj, StorageLevel.MEMORY_ONLY, tellMaster = false)          
          obj
        case None =>
          throw new SparkException(s"Failed to get broadcast " + broadcastId)
      }
    }
  }
}

private object CrailBroadcast {
  
}

object Utils {
  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        //        logError("Exception encountered", e)
        throw e
      case NonFatal(e) =>
        //        logError("Exception encountered", e)
        throw new IOException(e)
    }
  }
}

