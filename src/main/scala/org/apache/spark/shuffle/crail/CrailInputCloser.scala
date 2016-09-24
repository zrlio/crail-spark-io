package org.apache.spark.shuffle.crail

import com.ibm.crail.CrailInputStream

/**
 * Created by stu on 24.09.16.
 */
class CrailInputCloser[K, C](stream: CrailInputStream, baseIter: Iterator[Product2[K, C]]) extends Iterator[Product2[K, C]]{


  override def hasNext: Boolean = {
    baseIter.hasNext
  }

  override def next(): Product2[K, C] = {
    val product = baseIter.next()
    if (stream.available() == 0){
      stream.close()
    }
    product
  }
}
