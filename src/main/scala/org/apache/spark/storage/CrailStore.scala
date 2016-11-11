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

package org.apache.spark.storage

import java.io.OutputStream
import java.nio.ByteBuffer
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import java.util.concurrent.{ConcurrentHashMap, Future, LinkedBlockingQueue}

import com.ibm.crail._
import com.ibm.crail.conf.CrailConfiguration
import org.apache.spark._
import org.apache.spark.common._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.serializer.SerializationStream
import org.apache.spark.shuffle.crail.{CrailSerializationStream, CrailSerializerInstance}

import scala.util.Random


class CrailBlockFile (name: String, var file: CrailFile) {
  def getFile() : CrailFile = {
    return file
  }

  def update(newFile: CrailFile) = {
    file = newFile
  }
}


class CrailStore () extends Logging {
  val executorId: String = SparkEnv.get.executorId
  val conf = SparkEnv.get.conf
  val appId: String = conf.getAppId
  val serializer = SparkEnv.get.serializer

  val rootDir = "/spark"
  val broadcastDir = rootDir + "/broadcast"
  val shuffleDir = rootDir + "/shuffle"
  val rddDir = rootDir + "/rdd"
  val tmpDir = rootDir + "/tmp"
  val metaDir = rootDir + "/meta"
  val hostsDir = metaDir + "/hosts"

  var mapLocationAffinity : Boolean = _
  var deleteOnClose : Boolean = _
  var deleteOnStart : Boolean = _
  var outstanding : Int = _
  var preallocate : Int = 0
  var shuffleCycle : Int = _
  var writeAhead : Long = _
  var debug : Boolean = _
  var hostHash : Int = 0
  var isMap : AtomicBoolean = new AtomicBoolean(true)

  var fs : CrailFS = _
  var fileCache : ConcurrentHashMap[String, CrailBlockFile] = _
  var shuffleCache: ConcurrentHashMap[Integer, CrailShuffleStore] = _

  var fileGroupOpenStats = new AtomicLong(0)
  var streamGroupOpenStats = new AtomicLong(0)
  var fileGroupCloseStats = new AtomicLong(0)
  var streamGroupCloseStats = new AtomicLong(0)
  var multiStreamOpenStats = new AtomicLong(0)
  var multiStreamCloseStats = new AtomicLong(0)



  private def init(): Unit = {
    logInfo("CrailStore starting version 158")

    mapLocationAffinity = conf.getBoolean("spark.crail.shuffle.map.locationaffinity", true)
    deleteOnClose = conf.getBoolean("spark.crail.deleteonclose", false)
    deleteOnStart = conf.getBoolean("spark.crail.deleteonstart", true)
    outstanding = conf.getInt("spark.crail.shuffle.outstanding", 1)
    preallocate = conf.getInt("spark.crail.preallocate", 0)
    shuffleCycle = conf.getInt("spark.crail.shuffleCycle", Int.MaxValue)
    writeAhead = conf.getLong("spark.crail.writeAhead", 0)
    debug = conf.getBoolean("spark.crail.debug", false)

    logInfo("spark.crail.shuffle.affinity " + mapLocationAffinity)
    logInfo("spark.crail.deleteonclose " + deleteOnClose)
    logInfo("spark.crail.deleteOnStart " + deleteOnStart)
    logInfo("spark.crail.shuffle.outstanding " + outstanding)
    logInfo("spark.crail.preallocate " + preallocate)
    logInfo("spark.crail.shuffleCycle " + shuffleCycle)
    logInfo("spark.crail.writeAhead " + writeAhead)
    logInfo("spark.crail.debug " + debug)

    val crailConf = new CrailConfiguration();
    fs = CrailFS.newInstance(crailConf)
    fileCache = new ConcurrentHashMap[String, CrailBlockFile]()
    shuffleCache = new ConcurrentHashMap[Integer, CrailShuffleStore]()

    if (mapLocationAffinity){
      hostHash = fs.getHostHash
    }

    if (executorId == "driver"){
      logInfo("creating main dir " + rootDir)
      val baseDirExists : Boolean = fs.lookupNode(rootDir).get() != null

      if (!baseDirExists || deleteOnStart){
        logInfo("creating main dir " + rootDir)
        if (baseDirExists){
          fs.delete(rootDir, true).get().syncDir()
        }
        fs.makeDirectory(rootDir).get().syncDir()
        fs.makeDirectory(broadcastDir).get().syncDir()
        fs.makeDirectory(shuffleDir).get().syncDir()
        fs.makeDirectory(rddDir).get().syncDir()
        fs.makeDirectory(tmpDir).get().syncDir()
        fs.makeDirectory(metaDir).get().syncDir()
        fs.makeDirectory(hostsDir).get().syncDir()
        logInfo("creating main dir done " + rootDir)
      }
    }

    try {
      val hostFile = hostsDir + "/" + fs.getHostHash
      logInfo("creating hostFile " + hostFile)
      fs.createFile(hostFile, 0, 0).get().syncDir()
      logInfo("creating hostFile done " + hostFile)
    } catch {
      case e: Exception =>
        logInfo("exception e " + e.getMessage)
        e.printStackTrace()
    }

    try {
      logInfo("buffer cache warmup ")
      val tmpFile = tmpDir + "/" + Random.nextDouble()
      var file = fs.createFile(tmpFile, 0, 0).get()
      file.syncDir()
      var fileStream = file.getDirectOutputStream(0)
      val bufferQueue = new LinkedBlockingQueue[ByteBuffer]
      for( i <- 0 until preallocate){
        var buf = fs.allocateBuffer()
        bufferQueue.add(buf)
      }
      var buf = bufferQueue.poll()
      while (buf != null){
        buf.clear()
        fileStream.write(buf).get()
        fs.freeBuffer(buf)
        buf = bufferQueue.poll()
      }
      fileStream.close()
      fs.delete(tmpFile, false).get().syncDir()
      logInfo("buffer cache warmup done ")
    } catch {
      case e: Exception =>
        logInfo("exception e " + e.getMessage)
        e.printStackTrace()
    }

    fs.printStatistics("init")
    fs.resetStatistics()
  }

  def removeBlock(blockId: BlockId): Boolean = {
    //    val path = getPath(blockId.name);
    //    return fs.delete(path, true)
    true
  }

  def blockExists(blockId: BlockId): Boolean = {
    val crailFile = getLock(blockId)
    var ret : Boolean = false
    crailFile.synchronized {
      var fileInfo = crailFile.getFile()
      if (fileInfo == null) {
        val path = getPath(blockId)
        fileInfo = fs.lookupNode(path).get().asFile()
        crailFile.update(fileInfo)
      }
      if (fileInfo != null){
        ret = true
      }
    }
    return ret
  }

  def putBytes(blockId: BlockId, bytes: ByteBuffer): Unit = {
    val crailFile = getLock(blockId)
    crailFile.synchronized {
      var fileInfo = crailFile.getFile()
      if (fileInfo == null) {
        //        logInfo("fresh file, writing " + blockId.name)
        val path = getPath(blockId)
        try {
          fileInfo = fs.createFile(path, 0, 0).get()
          if (fileInfo != null && fileInfo.getCapacity() == 0) {
            val stream = fileInfo.getBufferedOutputStream(0)
            val byteBuffer = bytes.duplicate()
            byteBuffer.rewind()
            stream.write(byteBuffer)
            stream.close()
            crailFile.update(fileInfo)
          }
        } catch {
          case e: Exception =>
            //            logInfo("file already created, fetching update " + blockId.name)
            fileInfo = fs.lookupNode(path).get().asFile()
            crailFile.update(fileInfo)
        }
      }
    }
  }

  def putValues(blockId: BlockId, values: Iterator[_]): Unit = {
    val crailFile = getLock(blockId)
    crailFile.synchronized {
      var fileInfo = crailFile.getFile()
      if (fileInfo == null || (fileInfo != null && fileInfo.getToken() == 0)) {
        val path = getPath(blockId)
        try {
          fileInfo = fs.createFile(path, 0, 0).get()
          if (fileInfo != null && fileInfo.getCapacity() == 0) {
            val stream = fileInfo.getBufferedOutputStream(0)
            val instance = serializer.newInstance()
            val serializationStream = instance.serializeStream(stream)
            serializationStream.writeAll(values)
            crailFile.update(fileInfo)
          }
        } catch {
          case e: Exception =>
            //            logInfo("file already created, fetching update " + blockId.name)
            fileInfo = fs.lookupNode(path).get().asFile()
            crailFile.update(fileInfo)
        }
      }
    }
  }

  def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    val crailFile = getLock(blockId)
    var ret : Option[ByteBuffer] = None
    crailFile.synchronized {
      var fileInfo = crailFile.getFile()
      if (fileInfo == null){
        val path = getPath(blockId)
        fileInfo = fs.lookupNode(path).get().asFile()
        crailFile.update(fileInfo)
      }

      if (fileInfo != null && fileInfo.getCapacity() > 0) {
        val buffer = ByteBuffer.allocate(fileInfo.getCapacity().toInt)
        val stream = fileInfo.getBufferedInputStream(fileInfo.getCapacity)
        var sum = 0
        var break = 0
        while (break == 0 & sum < fileInfo.getCapacity()) {
          var ret = stream.read(buffer)
          if (ret > 0) {
            sum += ret
          } else {
            break = 1
          }
        }
        stream.close()
        buffer.clear();
        ret = Some(buffer)
      }
    }
    return ret
  }

  def getValues(blockId: BlockId): Option[Iterator[Any]] = {
    val crailFile = getLock(blockId)
    var ret : Option[Iterator[Any]] = None
    crailFile.synchronized {
      var fileInfo = crailFile.getFile()
      if (fileInfo == null){
        val path = getPath(blockId)
        fileInfo = fs.lookupNode(path).get().asFile()
        crailFile.update(fileInfo)
      }

      if (fileInfo != null && fileInfo.getCapacity() > 0){
        val stream = fileInfo.getBufferedInputStream(fileInfo.getCapacity)
        val instance = serializer.newInstance()
        val serializationStream = instance.deserializeStream(stream)
        val iter = serializationStream.asIterator
        ret = Some(iter)
      }
    }
    return ret
  }

  def getSize(blockId: BlockId): Long = {
    val crailFile = getLock(blockId)
    var ret : Long = 0
    crailFile.synchronized {
      var fileInfo = crailFile.getFile()
      if (fileInfo == null){
        val path = getPath(blockId)
        fileInfo = fs.lookupNode(path).get().asFile()
        crailFile.update(fileInfo)
      }

      if (fileInfo != null && fileInfo.getCapacity() > 0){
        ret = fileInfo.getCapacity()
      }
    }
    return ret
  }

  def isDebug() : Boolean = {
    return debug
  }

  def shutdown(): Unit = {
    logInfo("stopping CrailStore")
    if (fs != null){
      if (executorId == "driver"){
        if (deleteOnClose){
          fs.delete(rootDir, true).get()
        }
      }

      if (debug){
        //request by map task, if first (still in reduce state) then print reduce stats
        isMap.synchronized(
          if (isMap.get()){
            fs.printStatistics("map")
          } else {
            fs.printStatistics("reduce")
          }
        )
      }

      fs.close()
      fs.printStatistics("close")
    }
  }

  //---------------------------------------

  /* Register a shuffle with the manager and obtain a handle for it to pass to tasks. */
  def registerShuffle(shuffleId: Int, numMaps: Int, partitions: Int) : Unit = {
    //    logInfo("registering shuffle " + shuffleId + ", time " + ", cacheSize " + fs.getCacheSize)
    unregisterShuffle(shuffleId - shuffleCycle)
    if (shuffleCache.contains(shuffleId)){
      return
    }

    val shuffleStore = new CrailShuffleStore
    val oldStore = shuffleCache.putIfAbsent(shuffleId, shuffleStore)
    val futureQueue = new LinkedBlockingQueue[Future[CrailDirectory]]()
    val start = System.currentTimeMillis()
    val shuffleIdDir = shuffleDir + "/shuffle_" + shuffleId
    var future : Future[CrailDirectory] = fs.makeDirectory(shuffleIdDir)
    futureQueue.add(future)
    val i = 0
    for (i <- 0 until partitions){
      val subDir = shuffleIdDir + "/" + "part_" + i.toString
      future = fs.makeDirectory(subDir)
      futureQueue.add(future)
    }
    val fileQueue = new LinkedBlockingQueue[CrailDirectory]()
    while(!futureQueue.isEmpty){
      val file = futureQueue.poll().get()
      fileQueue.add(file)
    }
    while(!fileQueue.isEmpty){
      fileQueue.poll().syncDir()
    }
    val end = System.currentTimeMillis()
    val executionTime = (end - start) / 1000.0
  }

  /* Register a shuffle with the manager and obtain a handle for it to pass to tasks. */
  def unregisterShuffle(shuffleId: Int) : Unit = {
    try {
      if (shuffleId >= 0){
        if (!shuffleCache.contains(shuffleId)){
          return
        }
        val shuffleIdDir = shuffleDir + "/shuffle_" + shuffleId
        fs.delete(shuffleIdDir, true).get().syncDir()
        shuffleCache.remove(shuffleId)
      }
    } catch {
      case e: Exception =>
        logInfo("failed to unregister shuffle")
    }
  }

  private def getFileGroup(shuffleId: Int, numBuckets: Int) : CrailFileGroup = {
    var shuffleStore = shuffleCache.get(shuffleId)
    if (shuffleStore == null){
      shuffleStore = new CrailShuffleStore
      val oldStore = shuffleCache.putIfAbsent(shuffleId, shuffleStore)
      if (oldStore != null){
        shuffleStore = oldStore
      }
    }

    var fileGroup : CrailFileGroup = shuffleStore.getFileGroup(shuffleId, executorId, numBuckets, shuffleDir, fs, hostHash)
    return fileGroup
  }

  private def releaseFileGroup(shuffleId: Int, fileGroup: CrailFileGroup) = {
    var shuffleStore = shuffleCache.get(shuffleId)
    shuffleStore.releaseFileGroup(fileGroup)
  }

  def getWriterGroup(shuffleId: Int, numBuckets: Int, serializerInstance: CrailSerializerInstance, writeMetrics: ShuffleWriteMetrics) : CrailShuffleWriterGroup = {
    if (debug){
      //request by map task, if first (still in reduce state) then print reduce stats
      isMap.synchronized(
        if (isMap.compareAndSet(false, true)){
          fs.printStatistics("reduce")
        }
      )
    }

    streamGroupOpenStats.incrementAndGet()
    val fileGroup = getFileGroup(shuffleId, numBuckets)
    val shuffleGroup = new CrailShuffleWriterGroup(fs, fileGroup, shuffleId, serializerInstance, writeMetrics, writeAhead)
    return shuffleGroup
  }

  def releaseWriterGroup(shuffleId: Int, writerGroup: CrailShuffleWriterGroup) : Unit = {
    writerGroup.close()
    releaseFileGroup(shuffleId, writerGroup.fileGroup)
    streamGroupCloseStats.incrementAndGet()
  }

  def getMultiStream(shuffleId: Int, reduceId: Int, numMaps:Int) : CrailMultiStream = {
    if (debug){
      //request by map task, if first (still in reduce state) then print reduce stats
      isMap.synchronized(
        if (isMap.compareAndSet(true, false)){
          fs.printStatistics("map")
        }
      )
    }

    val name = shuffleDir + "/shuffle_" + shuffleId + "/part_" + reduceId + "/"
    val multiStream = fs.lookupNode(name).get().asDirectory().getMultiStream(outstanding)
    multiStreamOpenStats.incrementAndGet()

    return multiStream
  }

  //------------

  private def getLock(blockId: BlockId) : CrailBlockFile = {
    var crailFile = fileCache.get(blockId.name)
    if (crailFile == null){
      crailFile = new CrailBlockFile(blockId.name, null)
      val oldFile = fileCache.putIfAbsent(blockId.name, crailFile)
      if (oldFile != null){
        crailFile = oldFile
      }
    }
    return crailFile
  }

  private def getPath(blockId: BlockId): String = {
    var name = tmpDir + "/" + blockId.name
    if (blockId.isBroadcast){
      name = broadcastDir + "/" + blockId.name
    } else if (blockId.isShuffle){
      name = shuffleDir + "/" + blockId.name
    } else if (blockId.isRDD){
      name = rddDir + "/" + blockId.name
    }
    return name
  }

  private def printStats() : Unit = {
    logInfo("writerGroupOpen " + fileGroupOpenStats)
    logInfo("writerGroupCloseStats " + fileGroupCloseStats)
    logInfo("streamGroupOpenStats " + streamGroupOpenStats)
    logInfo("streamGroupCloseStats " + streamGroupCloseStats)
  }
}

class CrailFileGroup(val shuffleId: Int, val executorId: String, val fileId: Int, val writers: Array[CrailFile]){
}

class CrailShuffleStore{
  var store: LinkedBlockingQueue[CrailFileGroup] = new LinkedBlockingQueue[CrailFileGroup]()
  var size : AtomicInteger = new AtomicInteger(0)

  def getFileGroup(shuffleId: Int, executorId: String, numBuckets: Int, shuffleDir: String, fs: CrailFS, hostHash: Int) : CrailFileGroup = {
    var fileGroup = store.poll()
    if (fileGroup == null){
      store.synchronized{
        fileGroup = store.poll()
        if (fileGroup == null){
          val coreId = size.getAndIncrement()
          val futures: Array[Upcoming[CrailFile]] = new Array[Upcoming[CrailFile]](numBuckets)
          for (i <- 0 until numBuckets) {
            val filename = shuffleDir + "/shuffle_" + shuffleId + "/part_" + i + "/" + coreId + "-" + executorId + "-" + fs.getHostHash
            futures(i) = fs.createFile(filename, 0, hostHash)
          }
          val files: Array[CrailFile] = new Array[CrailFile](numBuckets)
          for (i <- 0 until numBuckets) {
            files(i) = futures(i).early()
          }
          fileGroup = new CrailFileGroup(shuffleId, executorId, coreId, files)
        }
      }
    }
    return fileGroup
  }

  def releaseFileGroup(fileGroup: CrailFileGroup) : Unit = {
    store.add(fileGroup)
  }
}

class CrailShuffleWriterGroup(val fs: CrailFS, val fileGroup: CrailFileGroup, shuffleId: Int, serializerInstance: CrailSerializerInstance, writeMetrics: ShuffleWriteMetrics, writeAhead: Long) extends Logging {
  val writers: Array[CrailObjectWriter] = new Array[CrailObjectWriter](fileGroup.writers.length)

  for (i <- 0 until fileGroup.writers.length){
    writers(i) = new CrailObjectWriter(fileGroup.writers(i), serializerInstance, writeMetrics, shuffleId, i, writeAhead)
  }

  def purge(): Unit = {
    for (i <- 0 until writers.length){
      if (writers(i).isOpen){
        writers(i).flushSerializer()
      }
    }

    val purgeQueue = new LinkedBlockingQueue[Future[_]]()
    for (i <- 0 until writers.length){
      if (writers(i).isOpen){
        val future = writers(i).purgeStream()
        purgeQueue.add(future)
      }
    }
    while(!purgeQueue.isEmpty){
      purgeQueue.poll().get()
    }

    val syncQueue = new LinkedBlockingQueue[Future[_]]()
    for (i <- 0 until writers.length){
      if (writers(i).isOpen){
        val future = writers(i).syncStream()
        syncQueue.add(future)
      }
    }
    while(!syncQueue.isEmpty){
      syncQueue.poll().get()
    }
  }

  def close(): Unit = {
    for (i <- 0 until writers.length){
      if (writers(i).isOpen){
        writers(i).close()
      }
    }
  }
}

object CrailStore extends Logging {
  private val lock = new Object()
  private var store: CrailStore = null
  private val shutdown : AtomicBoolean = new AtomicBoolean(false)

  def get: CrailStore = {
    if (store == null){
      lock.synchronized {
        if (store == null && !shutdown.get()){
          val _store = new CrailStore()
          _store.init()
          store = _store
        } else if (store == null && shutdown.get()){
          throw new Exception("Error: Attempt to re-initialize CrailStore (!)")
        }
      }
    }
    return store
  }

  def put : Unit = {
    if (store != null && !shutdown.get()){
      lock.synchronized{
        if (store != null && !shutdown.get()){
          store.shutdown()
          shutdown.set(true)
          store = null
        }
      }
    }
  }
}

private[spark] class CrailObjectWriter(file: CrailFile, serializerInstance: CrailSerializerInstance, writeMetrics: ShuffleWriteMetrics, shuffleId: Int, reduceId: Int, writeHint: Long)
  extends Logging
{
  private var initialized = false
  private var hasBeenClosed = false
  private var crailStream : CrailBufferedOutputStream = null
  private var serializationStream: CrailSerializationStream = null

  def open(): CrailObjectWriter = {
    if (hasBeenClosed) {
      throw new IllegalStateException("Writer already closed. Cannot be reopened.")
    }
    crailStream = file.getBufferedOutputStream(writeHint)
    serializationStream = serializerInstance.serializeCrailStream(crailStream)
    initialized = true
    this
  }

  def close() {
    if (initialized) {
      initialized = false
      serializationStream.close()
      hasBeenClosed = true
    }
  }

  def isOpen: Boolean = {
    return initialized
  }

  def write(key: Any, value: Any) {
    if (!initialized) {
      open()
    }

    serializationStream.writeKey(key)
    serializationStream.writeValue(value)
  }

  def length() : Long = {
    return crailStream.position()
  }

  def flushSerializer() {
    serializationStream.flush()
  }

  def purgeStream() : Future[CrailResult] = {
    crailStream.purge()
  }

  def syncStream() : Future[Void] = {
    crailStream.sync()
  }
}




