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

import com.ibm.crail.CrailMultiStream

/**
 * Created by stu on 24.09.16.
 */
class CrailInputCloser[K, C](deserializationStream: CrailDeserializationStream, baseIter: Iterator[Product2[K, C]]) extends Iterator[Product2[K, C]]{


  override def hasNext: Boolean = {
    baseIter.hasNext
  }

  override def next(): Product2[K, C] = {
    val product = baseIter.next()
//    if (stream.available() == 0){
//      stream.close()
//    }
    if (!hasNext) {
      deserializationStream.close()
    }    
    product
  }
}
