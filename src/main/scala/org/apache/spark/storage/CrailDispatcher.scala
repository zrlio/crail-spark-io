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

import java.nio.ByteBuffer
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import java.util.concurrent.{ConcurrentHashMap, Future, LinkedBlockingQueue, TimeUnit}

import org.apache.crail._
import org.apache.crail.conf.CrailConfiguration
import org.apache.crail.utils.CrailImmediateOperation
import org.apache.spark._
import org.apache.spark.common._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.serializer.{CrailSerializer, CrailSerializationStream, CrailSerializerInstance}
import org.apache.spark.util.Utils
import scala.reflect.ClassTag
import scala.util.Random


class CrailBlockFile (name: String, var file: CrailFile) {
  def getFile() : CrailFile = {
    return file
  }

  def update(newFile: CrailFile) = {
    file = newFile
  }
}


class CrailDispatcher () extends Logging {
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

  var deleteOnClose : Boolean = _
  var deleteOnStart : Boolean = _
  var preallocate : Int = 0
  var writeAhead : Long = _
  var debug : Boolean = _
  var crailSerializerClass : String = _
  var mapLocationAffinity : Boolean = _
  var outstanding : Int = _
  var shuffleStorageClass : CrailStorageClass = _
  var broadcastStorageClass : CrailStorageClass = _


  var fs : CrailStore = _
  var fileCache : ConcurrentHashMap[String, CrailBlockFile] = _
  var shuffleCache: ConcurrentHashMap[Integer, CrailShuffleStore] = _
  var crailSerializer : CrailSerializer = _
  var localLocationClass : CrailLocationClass = CrailLocationClass.DEFAULT
  var isMap : AtomicBoolean = new AtomicBoolean(true)

  var fileGroupOpenStats = new AtomicLong(0)
  var streamGroupOpenStats = new AtomicLong(0)
  var fileGroupCloseStats = new AtomicLong(0)
  var streamGroupCloseStats = new AtomicLong(0)
  var multiStreamOpenStats = new AtomicLong(0)
  var multiStreamCloseStats = new AtomicLong(0)



  private def init(): Unit = {
    logInfo("CrailStore starting version 400")

    deleteOnClose = conf.getBoolean("spark.crail.deleteonclose", false)
    deleteOnStart = conf.getBoolean("spark.crail.deleteonstart", true)
    preallocate = conf.getInt("spark.crail.preallocate", 0)
    writeAhead = conf.getLong("spark.crail.writeAhead", 0)
    debug = conf.getBoolean("spark.crail.debug", false)
    crailSerializerClass = conf.get("spark.crail.serializer", "org.apache.spark.serializer.CrailSparkSerializer")

    mapLocationAffinity = conf.getBoolean("spark.crail.shuffle.map.locationaffinity", true)
    outstanding = conf.getInt("spark.crail.shuffle.outstanding", 1)
    shuffleStorageClass = CrailStorageClass.get(conf.getInt("spark.crail.shuffle.storageclass", 0))
    broadcastStorageClass = CrailStorageClass.get(conf.getInt("spark.crail.broadcast.storageclass", 0))

    logInfo("spark.crail.deleteonclose " + deleteOnClose)
    logInfo("spark.crail.deleteOnStart " + deleteOnStart)
    logInfo("spark.crail.preallocate " + preallocate)
    logInfo("spark.crail.writeAhead " + writeAhead)
    logInfo("spark.crail.debug " + debug)
    logInfo("spark.crail.serializer " + crailSerializerClass)
    logInfo("spark.crail.shuffle.affinity " + mapLocationAffinity)
    logInfo("spark.crail.shuffle.outstanding " + outstanding)
    logInfo("spark.crail.shuffle.storageclass " + shuffleStorageClass.value())
    logInfo("spark.crail.broadcast.storageclass " + broadcastStorageClass.value())

    val crailConf = new CrailConfiguration();
    fs = CrailStore.newInstance(crailConf)
    fileCache = new ConcurrentHashMap[String, CrailBlockFile]()
    shuffleCache = new ConcurrentHashMap[Integer, CrailShuffleStore]()
    crailSerializer = Utils.classForName(crailSerializerClass).newInstance.asInstanceOf[CrailSerializer]

    if (mapLocationAffinity){
      localLocationClass = fs.getLocationClass()
    }

    if (executorId == "driver"){
      logInfo("creating main dir " + rootDir)
      val baseDirExists : Boolean = fs.lookup(rootDir).get() != null

      if (!baseDirExists || deleteOnStart){
        logInfo("creating main dir " + rootDir)
        if (baseDirExists){
          fs.delete(rootDir, true).get().syncDir()
        }
        fs.create(rootDir, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get().syncDir()
        fs.create(broadcastDir, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get().syncDir()
        fs.create(shuffleDir, CrailNodeType.DIRECTORY, shuffleStorageClass, CrailLocationClass.DEFAULT, true).get().syncDir()
        fs.create(rddDir, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get().syncDir()
        fs.create(tmpDir, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get().syncDir()
        fs.create(metaDir, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get().syncDir()
        fs.create(hostsDir, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get().syncDir()
        logInfo("creating main dir done " + rootDir)
      }
    }

    try {
      val hostFile = hostsDir + "/" + fs.getLocationClass().value();
      logInfo("creating hostFile " + hostFile)
      fs.create(hostFile, CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get().syncDir()
      logInfo("creating hostFile done " + hostFile)
    } catch {
      case e: Exception =>
        logInfo("exception e " + e.getMessage)
        e.printStackTrace()
    }

    try {
      logInfo("buffer cache warmup ")
      val tmpFile = tmpDir + "/" + Random.nextDouble()
      var file = fs.create(tmpFile, CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get().asFile()
      file.syncDir()
      var fileStream = file.getDirectOutputStream(0)
      val bufferQueue = new LinkedBlockingQueue[CrailBuffer]
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

    fs.getStatistics.print("init")
    fs.getStatistics.reset()
  }

  def getCrailSerializer(): CrailSerializer = {
    return crailSerializer
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
        fileInfo = fs.lookup(path).get().asFile()
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
          fileInfo = fs.create(path, CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get().asFile()
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
            fileInfo = fs.lookup(path).get().asFile()
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
          fileInfo = fs.create(path, CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get().asFile()
          if (fileInfo != null && fileInfo.getCapacity() == 0) {
            val stream = fileInfo.getBufferedOutputStream(0)
            val instance = serializer.newInstance()
            val serializationStream = instance.serializeStream(stream)
            serializationStream.writeAll(values)
            serializationStream.close();
            crailFile.update(fileInfo)
          }
        } catch {
          case e: Exception =>
            //            logInfo("file already created, fetching update " + blockId.name)
            fileInfo = fs.lookup(path).get().asFile()
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
        fileInfo = fs.lookup(path).get().asFile()
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
        fileInfo = fs.lookup(path).get().asFile()
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
        fileInfo = fs.lookup(path).get().asFile()
        crailFile.update(fileInfo)
      }

      if (fileInfo != null && fileInfo.getCapacity() > 0){
        ret = fileInfo.getCapacity()
      }
    }
    return ret
  }

  def writeBroadcast[T: ClassTag](blockId: BlockId, value: T): Unit = {
    val path = getPath(blockId)
    try {
      val fileInfo = fs.create(path, CrailNodeType.DATAFILE, broadcastStorageClass, CrailLocationClass.DEFAULT, true).get().asFile()
      val stream = fileInfo.getBufferedOutputStream(0)
      val serializationStream = crailSerializer.newCrailSerializer(serializer).serializeCrailStream(stream)
      serializationStream.writeBroadcast(value)
      serializationStream.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  private def readBroadcastDebug(id: Long, blockId: BlockId): Option[Any] = {
    val path = getPath(blockId)
    val s1 = System.nanoTime()
    val fileInfo = fs.lookup(path).get().asFile()
    val s2 = System.nanoTime()
    val stream = fileInfo.getBufferedInputStream(fileInfo.getCapacity)
    val s3 = System.nanoTime()
    val deserializationStream = crailSerializer.newCrailSerializer(serializer).deserializeCrailStream(stream)
    val value = Some(deserializationStream.readBroadcast())
    deserializationStream.close()
    val s4 = System.nanoTime()
    logInfo("Deserialization broadcast " + id + ": lookup " + ( s2 - s1)/ 1000 + " usec " +
      " getBufferedStream " + (s3 - s2) / 1000 + " usec " +
      " readObject2 " +  (s4 - s3) / 1000 + " usec " +
      " end2end " + (s4 - s1) / 1000 + " usec , (size: " + fileInfo.getCapacity +
      " bytes) , class type: " + value.get.getClass.getCanonicalName)
    value
  }

  def readBroadcast(id: Long, blockId: BlockId): Option[Any] = {
    if(!debug) {
      val fileInfo = fs.lookup(getPath(blockId)).get().asFile()
      val stream = fileInfo.getBufferedInputStream(fileInfo.getCapacity)
      val deserializationStream = crailSerializer.newCrailSerializer(serializer).deserializeCrailStream(stream)
      val value = Some(deserializationStream.readBroadcast())
      deserializationStream.close()
      value
    } else {
      readBroadcastDebug(id, blockId)
    }
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
            fs.getStatistics.print("map")
          } else {
            fs.getStatistics.print("reduce")
          }
        )
      }

      fs.close()
      fs.getStatistics.print("close")
    }
  }

  //---------------------------------------

  /* Register a shuffle with the manager and obtain a handle for it to pass to tasks. */
  def registerShuffle(shuffleId: Int, numMaps: Int, partitions: Int) : Unit = {
    //logInfo("registering shuffle " + shuffleId + ", time " + ", cacheSize " + fs.getCacheSize)
    val shuffleStore = new CrailShuffleStore
    val oldStore = shuffleCache.putIfAbsent(shuffleId, shuffleStore)
    val futureQueue = new LinkedBlockingQueue[Future[CrailNode]]()
    val start = System.currentTimeMillis()
    val shuffleIdDir = shuffleDir + "/shuffle_" + shuffleId
    var future : Future[CrailNode] = fs.create(shuffleIdDir, CrailNodeType.DIRECTORY, CrailStorageClass.PARENT, CrailLocationClass.DEFAULT, true)
    futureQueue.add(future)
    val i = 0
    for (i <- 0 until partitions){
      val subDir = shuffleIdDir + "/" + "part_" + i.toString
      future = fs.create(subDir, CrailNodeType.MULTIFILE, CrailStorageClass.PARENT, CrailLocationClass.DEFAULT, true)
      futureQueue.add(future)
    }
    val fileQueue = new LinkedBlockingQueue[CrailNode]()
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
      val shuffleIdDir = shuffleDir + "/shuffle_" + shuffleId
      fs.delete(shuffleIdDir, true).get().syncDir()
      shuffleCache.remove(shuffleId)
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

    var fileGroup : CrailFileGroup = shuffleStore.getFileGroup(shuffleId, executorId, numBuckets, shuffleDir, fs, localLocationClass)
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
          fs.getStatistics.print("reduce")
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

  def getMultiStream(shuffleId: Int, reduceId: Int, numMaps:Int) : CrailBufferedInputStream = {
    if (debug){
      //request by map task, if first (still in reduce state) then print reduce stats
      isMap.synchronized(
        if (isMap.compareAndSet(true, false)){
          fs.getStatistics.print("map")
        }
      )
    }

    val name = shuffleDir + "/shuffle_" + shuffleId + "/part_" + reduceId + "/"
    val multiStream = fs.lookup(name).get().asMultiFile().getMultiStream(outstanding)
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

class CrailFileGroup(val shuffleId: Int, val executorId: String, val fileId: Int, val writers: Array[CrailNode]){
}

class CrailShuffleStore{
  var store: LinkedBlockingQueue[CrailFileGroup] = new LinkedBlockingQueue[CrailFileGroup]()
  var size : AtomicInteger = new AtomicInteger(0)

  def getFileGroup(shuffleId: Int, executorId: String, numBuckets: Int, shuffleDir: String, fs: CrailStore, locationClass: CrailLocationClass) : CrailFileGroup = {
    var fileGroup = store.poll()
    if (fileGroup == null){
      store.synchronized{
        fileGroup = store.poll()
        if (fileGroup == null){
          val coreId = size.getAndIncrement()
          val futures: Array[Upcoming[CrailNode]] = new Array[Upcoming[CrailNode]](numBuckets)
          for (i <- 0 until numBuckets) {
            val filename = shuffleDir + "/shuffle_" + shuffleId + "/part_" + i + "/" + coreId + "-" + executorId + "-" + fs.getLocationClass().value()
            futures(i) = fs.create(filename, CrailNodeType.DATAFILE, CrailStorageClass.PARENT, CrailLocationClass.DEFAULT, true)
          }
          val files: Array[CrailNode] = new Array[CrailNode](numBuckets)
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

class CrailShuffleWriterGroup(val fs: CrailStore, val fileGroup: CrailFileGroup, shuffleId: Int, serializerInstance: CrailSerializerInstance, writeMetrics: ShuffleWriteMetrics, writeAhead: Long) extends Logging {
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
      writers(i).close()
    }
  }
}

object CrailDispatcher extends Logging {
  private val lock = new Object()
  private var store: CrailDispatcher = null
  private val shutdown : AtomicBoolean = new AtomicBoolean(false)

  def get: CrailDispatcher = {
    if (store == null){
      lock.synchronized {
        if (store == null && !shutdown.get()){
          val _store = new CrailDispatcher()
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

private[spark] class CrailObjectWriter(file: CrailNode, serializerInstance: CrailSerializerInstance, writeMetrics: ShuffleWriteMetrics, shuffleId: Int, reduceId: Int, writeHint: Long)
  extends Logging
{
  private var initialized = false
  private var hasBeenClosed = false
  private var crailStream : CrailBufferedOutputStream = null
  private var serializationStream: CrailSerializationStream = null
  private val noPurge : CrailImmediateOperation = new CrailImmediateOperation(0)
  private val noSync : NoSync = new NoSync()

  def open(): CrailObjectWriter = {
    if (hasBeenClosed) {
      throw new IllegalStateException("Writer already closed. Cannot be reopened.")
    }
    crailStream = file.asFile().getBufferedOutputStream(writeHint)
    serializationStream = serializerInstance.serializeCrailStream(crailStream)
    initialized = true
    this
  }

  def close() {
    if (initialized) {
      initialized = false
      serializationStream.close()
      hasBeenClosed = true
    } else {
      file.syncDir()
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
    if (initialized) {
      return crailStream.position()
    } else {
      return 0
    }
  }

  def flushSerializer() {
    if (initialized) {
      serializationStream.flush()
    }
  }

  def purgeStream() : Future[CrailResult] = {
    if (initialized) {
      crailStream.purge()
    } else {
      return noPurge
    }
  }

  def syncStream() : Future[Void] = {
    if (initialized) {
      crailStream.sync()
    } else {
      return noSync
    }
  }
}

class NoSync extends Future[Void] {
  override def isCancelled: Boolean = false
  override def get(): Void = null
  override def get(timeout: Long, unit: TimeUnit): Void = null
  override def cancel(mayInterruptIfRunning: Boolean): Boolean = false
  override def isDone: Boolean = true
}




