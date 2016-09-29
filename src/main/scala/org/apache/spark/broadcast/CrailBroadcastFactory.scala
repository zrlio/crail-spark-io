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

import org.apache.spark.{SecurityManager, SparkConf}

import scala.reflect.ClassTag


class CrailBroadcastFactory extends BroadcastFactory {

//  logInfo("rdfs broadcast started, version 1")

  override def initialize(isDriver: Boolean, conf: SparkConf, securityMgr: SecurityManager) { }

  override def newBroadcast[T: ClassTag](value_ : T, isLocal: Boolean, id: Long) = {
    new CrailBroadcast[T](value_, id)
  }


  override def stop() { }

  /**
   * Remove all persisted state associated with the torrent broadcast with the given ID.
   * @param removeFromDriver Whether to remove state from the driver.
   * @param blocking Whether to block until unbroadcasted
   */
  override def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean) {
  }
}

