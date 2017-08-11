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

import org.apache.spark.storage._
import org.apache.spark.{SparkEnv, SparkException}

import scala.collection.mutable
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
  
  override protected def doUnpersist(blocking: Boolean): Unit = {
    logWarning(" called doUnpersist on broadcastId: " + id + " (NYI)")
  }  
  
  override protected def doDestroy(blocking: Boolean): Unit = {
    /* this is the chance where we can delete the broadcast file */
    logWarning(" called doDestroy on broadcastId: " + id + " (NYI)")
  }
  
  private def writeBlocks(value: T) {
    CrailStore.get.writeBroadcast(broadcastId, value)
  }

  private def readBroadcastBlock(): T = Utils.tryOrIOException {
    CrailBroadcast.synchronized {
      /* first lets find out if we have it locally cached */
      val broadcast = if(CrailBroadcast.useLocalCache) {
        CrailBroadcast.broadcastCache.getOrElse(id, None)
      } else {
        SparkEnv.get.blockManager.getLocalValues(broadcastId).map(_.data.next())
      }
      broadcast match {
        /* we got it */
        case Some(x) => x.asInstanceOf[T]
        /* we don't have it */
        case None =>
          val bc = CrailStore.get.readBroadcast(id, broadcastId)
          bc match {
            case Some(x) => /* we read it and now put in the local store */
              val obj = x.asInstanceOf[T]
              if(CrailBroadcast.useLocalCache) {
                CrailBroadcast.broadcastCache(id) = Some(x)
              } else {
                SparkEnv.get.blockManager.putSingle(broadcastId, obj, StorageLevel.MEMORY_ONLY, tellMaster = false)
              }
              obj
            case None =>
              throw new SparkException(s"Failed to get broadcast " + broadcastId)
          }
      }
    }
  }
}

private object CrailBroadcast {

  //FIXME: (atr) I am not completely sure about if this gives us the best performance.
  val broadcastCache:mutable.HashMap[Long, Option[Any]] = new mutable.HashMap[Long, Option[Any]]
  private val useLocalCache = false

  def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit = {
    this.synchronized {
      if(useLocalCache) {
        broadcastCache.remove(id)
      } else {
        SparkEnv.get.blockManager.master.removeBroadcast(id, removeFromDriver, blocking)
        SparkEnv.get.blockManager.removeBroadcast(id, false)
      }
    }
  }

  def cleanCache(): Unit = {
    this.synchronized {
      broadcastCache.clear()
    }
  }
}

object Utils {
  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        throw e
      case NonFatal(e) =>
        throw new IOException(e)
    }
  }
}

