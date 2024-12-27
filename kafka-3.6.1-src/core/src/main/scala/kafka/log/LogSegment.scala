/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.log

import com.yammer.metrics.core.Timer
import kafka.common.LogSegmentOffsetOverflowException
import kafka.utils._
import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.record.FileRecords.{LogOffsetPosition, TimestampAndOffset}
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.{BufferSupplier, Time, Utils}
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache
import org.apache.kafka.storage.internals.log.{AbortedTxn, AppendOrigin, CompletedTxn, FetchDataInfo, LazyIndex, LogConfig, LogOffsetMetadata, OffsetIndex, OffsetPosition, ProducerStateManager, RollParams, TimeIndex, TimestampOffset, TransactionIndex, TxnIndexSearchResult}

import java.io.{File, IOException}
import java.nio.file.attribute.FileTime
import java.nio.file.{Files, NoSuchFileException}
import java.util.Optional
import java.util.concurrent.TimeUnit
import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._
import scala.math._

/**
 * A segment of the log. Each segment has two components: a log and an index. The log is a FileRecords containing
 * the actual messages. The index is an OffsetIndex that maps from logical offsets to physical file positions. Each
 * segment has a base offset which is an offset <= the least offset of any message in this segment and > any offset in
 * any previous segment.
 *
 * A segment with a base offset of [base_offset] would be stored in two files, a [base_offset].index and a [base_offset].log file.
 *
 * @param log The file records containing log entries
 * @param lazyOffsetIndex The offset index
 * @param lazyTimeIndex The timestamp index
 * @param txnIndex The transaction index
 * @param baseOffset A lower bound on the offsets in this segment
 * @param indexIntervalBytes The approximate number of bytes between entries in the index
 * @param rollJitterMs The maximum random jitter subtracted from the scheduled segment roll time
 * @param time The time instance
 */
@nonthreadsafe
class LogSegment private[log] (val log: FileRecords,
                               val lazyOffsetIndex: LazyIndex[OffsetIndex],
                               val lazyTimeIndex: LazyIndex[TimeIndex],
                               val txnIndex: TransactionIndex,
                               val baseOffset: Long,
                               val indexIntervalBytes: Int, // 新写入多少字节后添加一个index(默认4096 bytes)
                               val rollJitterMs: Long, // 最大随机抖动时间，避免同时创建多个LogSegment，缓解磁盘I/O压力
                               val time: Time) extends Logging {

  def offsetIndex: OffsetIndex = lazyOffsetIndex.get

  def timeIndex: TimeIndex = lazyTimeIndex.get

  /**
   * Determine whether a new LogSegment needs to be created
   * @param rollParams: hold params required to decide to rotate a log segment or not
   * @return
   */
  def shouldRoll(rollParams: RollParams): Boolean = {
    // 1. check whether reached "RollMs" ?
    // 计算已经多久没 Roll 出一个新的 LogSegment 了
    // [“最新” 与 “最旧” 间的时间差] > [LogSegment最大"存活"时间 - 随机抖动时间] ?
    val reachedRollMs = timeWaitedForRoll(rollParams.now, rollParams.maxTimestampInMessages) > rollParams.maxSegmentMs - rollJitterMs

    // 2. judge: five situations...
    // case1：log空间层面的滚动
    // 下面这个表达式换种写法，可能更好理解
    // size(LogSegment现在已经写了多少bytes数据) + rollParams.messagesSize(还想往这个LogSegment里写多少数据) > rollParams.maxSegmentBytes(LogSegment 最多能写多少数据)
    size > rollParams.maxSegmentBytes - rollParams.messagesSize ||
    // case2：log时间层面的滚动
      (size > 0 && reachedRollMs) ||
    // case3：index空间层面的滚动
      offsetIndex.isFull ||
      timeIndex.isFull ||
    // case4: 新增的offset(maxOffsetInMessages) 和 baseOffset 之间的差距不合理
      !canConvertToRelativeOffset(rollParams.maxOffsetInMessages)
  }

  def resizeIndexes(size: Int): Unit = {
    offsetIndex.resize(size)
    timeIndex.resize(size)
  }

  def sanityCheck(timeIndexFileNewlyCreated: Boolean): Unit = {
    if (lazyOffsetIndex.file.exists) {
      // Resize the time index file to 0 if it is newly created.
      if (timeIndexFileNewlyCreated)
        timeIndex.resize(0)
      // Sanity checks for time index and offset index are skipped because
      // we will recover the segments above the recovery point in recoverLog()
      // in any case so sanity checking them here is redundant.
      txnIndex.sanityCheck()
    }
    else throw new NoSuchFileException(s"Offset index file ${lazyOffsetIndex.file.getAbsolutePath} does not exist")
  }

  private var created = time.milliseconds

  /* the number of bytes since we last added an entry in the offset index */
  // 自上次添加index之后写入了多少bytes，会和成员变量 indexIntervalBytes 比较
  private var bytesSinceLastIndexEntry = 0

  // The timestamp(时间戳) we used for(用于) [ "time based log rolling" ] and for [ "ensuring max compaction delay" ]
  // volatile for LogCleaner to see the update
  @volatile private var rollingBasedTimestamp: Option[Long] = None

  /* The maximum timestamp and offset we see so far */
  // 已追加Record的最大时间戳
  @volatile private var _maxTimestampAndOffsetSoFar: TimestampOffset = TimestampOffset.UNKNOWN
  def maxTimestampAndOffsetSoFar_= (timestampOffset: TimestampOffset): Unit = _maxTimestampAndOffsetSoFar = timestampOffset

  // timeIndex索引的最后一个entry封的对象 TimestampOffset : [timestamp - offset]
  def maxTimestampAndOffsetSoFar: TimestampOffset = {
    if (_maxTimestampAndOffsetSoFar == TimestampOffset.UNKNOWN)
      _maxTimestampAndOffsetSoFar = timeIndex.lastEntry
    _maxTimestampAndOffsetSoFar
  }

  /* The maximum timestamp we see so far */
  def maxTimestampSoFar: Long = {
    maxTimestampAndOffsetSoFar.timestamp
  }

  def offsetOfMaxTimestampSoFar: Long = {
    maxTimestampAndOffsetSoFar.offset
  }

  /* Return the size in bytes of this log segment */
  def size: Int = log.sizeInBytes()

  /**
   * checks that the argument offset can be represented as an integer offset relative to the baseOffset.
   */
  def canConvertToRelativeOffset(offset: Long): Boolean = {
    offsetIndex.canAppendOffset(offset)
  }

  /**
   * Append the given messages starting with the given offset. Add
   * an entry to the index if needed.
   *
   * It is assumed this method is being called from within a lock.
   *
   * @param largestOffset The last offset in the message set
   * @param largestTimestamp The largest timestamp in the message set.
   * @param shallowOffsetOfMaxTimestamp The offset of the message that has the largest timestamp in the messages to append.
   * @param records The log entries to append.
   *                入参的records是MemoryRecords类型， LogSegment自身维护的是FileRecords类型，FileRecords有 append(MemoryRecords records)方法
   * @throws LogSegmentOffsetOverflowException if the largest offset causes index offset overflow
   */
  @nonthreadsafe // 非线程安全的，上层调用的地方，会加锁
  def append(largestOffset: Long,
             largestTimestamp: Long,
             shallowOffsetOfMaxTimestamp: Long, // 最大时间戳对应的Offset
             records: MemoryRecords): Unit = {
    // It will only be called when records > 0
    if (records.sizeInBytes > 0) {
      trace(s"Inserting ${records.sizeInBytes} bytes at end offset $largestOffset at position ${log.sizeInBytes} " +
            s"with largest timestamp $largestTimestamp at shallow offset $shallowOffsetOfMaxTimestamp")

      // 1. get physical Position, help for offsetIndex
      // 所谓physicalPosition，其实就是这个文件内有多少Byte
      val physicalPosition = log.sizeInBytes()
      // physicalPosition == 0， 意味着，这个LogSegment已经没有任何数据了，所以 以此次添加的records中的最大时间戳，作为”rollingBasedTimestamp“
      // 简单来说就是， 下次logsegment滚动，以这个时间为计算基础了
      if (physicalPosition == 0)
        rollingBasedTimestamp = Some(largestTimestamp)

      // 校验新增的offset， 验证条件就是
      // long relativeOffset = largestOffset - baseOffset;
      // if (relativeOffset < 0 || relativeOffset > Integer.MAX_VALUE)
      ensureOffsetInRange(largestOffset)

      // 2. * FileRecords: append the messages
      val appendedBytes = log.append(records)
      trace(s"Appended $appendedBytes to ${log.file} at end offset $largestOffset")

      // 更新 内存中维护的 TimestampOffset [最大时间戳 - 对应offset],
      // Update the in memory max timestamp and corresponding offset.
      if (largestTimestamp > maxTimestampSoFar) {
        maxTimestampAndOffsetSoFar = new TimestampOffset(largestTimestamp, shallowOffsetOfMaxTimestamp)
      }

      // 3. offsetIndex/timeIndex: append an entry to the index (if needed)
      // 需要添加索引了
      if (bytesSinceLastIndexEntry > indexIntervalBytes) {
        offsetIndex.append(largestOffset, physicalPosition)
        timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar)
        bytesSinceLastIndexEntry = 0
      }

      // 4. update bytesSinceLastIndexEntry 计算新写了多少bytes的数据
      bytesSinceLastIndexEntry += records.sizeInBytes
    }
  }

  private def ensureOffsetInRange(offset: Long): Unit = {
    if (!canConvertToRelativeOffset(offset))
      throw new LogSegmentOffsetOverflowException(this, offset)
  }

  private def appendChunkFromFile(records: FileRecords, position: Int, bufferSupplier: BufferSupplier): Int = {
    var bytesToAppend = 0
    var maxTimestamp = Long.MinValue
    var offsetOfMaxTimestamp = Long.MinValue
    var maxOffset = Long.MinValue
    var readBuffer = bufferSupplier.get(1024 * 1024)

    def canAppend(batch: RecordBatch) =
      canConvertToRelativeOffset(batch.lastOffset) &&
        (bytesToAppend == 0 || bytesToAppend + batch.sizeInBytes < readBuffer.capacity)

    // find all batches that are valid to be appended to the current log segment and
    // determine the maximum offset and timestamp
    val nextBatches = records.batchesFrom(position).asScala.iterator
    for (batch <- nextBatches.takeWhile(canAppend)) {
      if (batch.maxTimestamp > maxTimestamp) {
        maxTimestamp = batch.maxTimestamp
        offsetOfMaxTimestamp = batch.lastOffset
      }
      maxOffset = batch.lastOffset
      bytesToAppend += batch.sizeInBytes
    }

    if (bytesToAppend > 0) {
      // Grow buffer if needed to ensure we copy at least one batch
      if (readBuffer.capacity < bytesToAppend)
        readBuffer = bufferSupplier.get(bytesToAppend)

      readBuffer.limit(bytesToAppend)
      records.readInto(readBuffer, position)

      append(maxOffset, maxTimestamp, offsetOfMaxTimestamp, MemoryRecords.readableRecords(readBuffer))
    }

    bufferSupplier.release(readBuffer)
    bytesToAppend
  }

  /**
   * Append records from a file beginning at the given position until either the end of the file
   * is reached or an offset is found which is too large to convert to a relative offset for the indexes.
   *
   * @return the number of bytes appended to the log (may be less than the size of the input if an
   *         offset is encountered which would overflow this segment)
   */
  def appendFromFile(records: FileRecords, start: Int): Int = {
    var position = start
    val bufferSupplier: BufferSupplier = new BufferSupplier.GrowableBufferSupplier
    while (position < start + records.sizeInBytes) {
      val bytesAppended = appendChunkFromFile(records, position, bufferSupplier)
      if (bytesAppended == 0)
        return position - start
      position += bytesAppended
    }
    position - start
  }

  @nonthreadsafe
  def updateTxnIndex(completedTxn: CompletedTxn, lastStableOffset: Long): Unit = {
    if (completedTxn.isAborted) {
      trace(s"Writing aborted transaction $completedTxn to transaction index, last stable offset is $lastStableOffset")
      // "xxxx.txnindex" can help consumers filter "abort Txn messages"
      txnIndex.append(new AbortedTxn(completedTxn, lastStableOffset))
    }
  }

  private def updateProducerState(producerStateManager: ProducerStateManager, batch: RecordBatch): Unit = {
    if (batch.hasProducerId) {
      val producerId = batch.producerId
      val appendInfo = producerStateManager.prepareUpdate(producerId, AppendOrigin.REPLICATION)
      val maybeCompletedTxn = appendInfo.append(batch, Optional.empty())
      producerStateManager.update(appendInfo)
      maybeCompletedTxn.ifPresent(completedTxn => {
        val lastStableOffset = producerStateManager.lastStableOffset(completedTxn)
        // may write "Txn attach data" to "xxxx.txnindex"
        updateTxnIndex(completedTxn, lastStableOffset)
        producerStateManager.completeTxn(completedTxn)
      })
    }
    producerStateManager.updateMapEndOffset(batch.lastOffset + 1)
  }

  /**
   * Find the physical file position for the first message with offset >= the requested offset.
   *
   * The startingFilePosition argument is an optimization that can be used if we already know a valid starting position
   * in the file higher than the greatest-lower-bound from the index.
   *
   * @param offset The offset we want to translate
   * @param startingFilePosition A lower bound on the file position from which to begin the search. This is purely an optimization and
   * when omitted, the search will begin at the position in the offset index.
   * @return The position in the log storing the message with the least offset >= the requested offset and the size of the
    *        message or null if no message meets this criteria.
   */
  @threadsafe
  private[log] def translateOffset(offset: Long, startingFilePosition: Int = 0): LogOffsetPosition = {
    // first search: search from index
    val mapping = offsetIndex.lookup(offset)
    // second search：二次寻找，会以第一次找到的position为起始点
    log.searchForOffsetWithSize(offset, max(mapping.position, startingFilePosition))
  }

  /**
   * Read a message set from this segment beginning with the first offset >= startOffset. The message set will include
   * no more than maxSize bytes and will end before maxOffset if a maxOffset is specified.
   *
   * @param startOffset A lower bound on the "first offset" to include in the message set we read
   * @param maxSize The maximum number of bytes to include in the message set we read
   * @param maxPosition The maximum position in the log segment that should be exposed for read
   * @param minOneMessage If this is true, the first message will be returned even if it exceeds `maxSize` (if one exists)
   *
   * @return The fetched data and the offset metadata of the first message whose offset is >= startOffset,
   *         or null if the startOffset is larger than the largest offset in this log
   */
  @threadsafe
  def read(startOffset: Long, // 读取时的目标起始offset
           maxSize: Int, // 最大读多少bytes
           maxPosition: Long = size, // logsegment中最大到哪个byte是可以读的
           minOneMessage: Boolean = false // 是否要求至少返回一条message
          ): FetchDataInfo = {
    if (maxSize < 0)
      throw new IllegalArgumentException(s"Invalid max size $maxSize for log read from segment $log")

    // 利用index文件查找， 最后返回 LogOffsetPosition-[offset, position, size] 对象
    val startOffsetAndSize = translateOffset(startOffset)

    // if the start position is already off the end of the log, return null
    if (startOffsetAndSize == null)
      return null

    val startPosition = startOffsetAndSize.position
    val offsetMetadata = new LogOffsetMetadata(startOffset, this.baseOffset, startPosition)

    // "minOneMessage" means "At least one message needs to be obtained"
    val adjustedMaxSize =
      if (minOneMessage) math.max(maxSize, startOffsetAndSize.size)
      else maxSize

    // return a log segment but with zero size in the case below
    if (adjustedMaxSize == 0)
      return new FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY)

    // calculate the length of the message set to read based on whether or not they gave us a maxOffset
    // [最大能读取的位置maxPosition - 最小想读取的位置 = 读取的bytes大小]
    val fetchSize: Int = min((maxPosition - startPosition).toInt, adjustedMaxSize)

    // use "FileRecords.slice(int position, int size)" to fetch data
    new FetchDataInfo(offsetMetadata, log.slice(startPosition, fetchSize),
      adjustedMaxSize < startOffsetAndSize.size, Optional.empty())
  }

   def fetchUpperBoundOffset(startOffsetPosition: OffsetPosition, fetchSize: Int): Optional[Long] =
     offsetIndex.fetchUpperBoundOffset(startOffsetPosition, fetchSize).map(_.offset)

  /**
   * 代价很高的一个方法，
   * recover的对象是完整的某个logsegment从头开始，而不是从某个logsegment的某个offset开始
   * 过程包含：
   * 1. RecordBatch的消息内容CRC校验(消息刷写到磁盘有无出错)
   * 2. Index的清空与重建
   * 3. FileRecords以某位置（发现异常的位置）开始截断
   *
   * Run recovery on the given segment. This will rebuild the index from the log file and lop off any invalid bytes(删除无效字节)
   * from the end of the log and index.
   *
   * only recover logSegments after recoveryPointCheckpoint, not all logSegments (只会从 recoveryPointCheckpoint 之后 logSegments 开始 recover)
   *
   * @param producerStateManager Producer state corresponding to the segment's base offset. This is needed to recover
   *                             the transaction index. [重建 transaction index 用的]
   * @param leaderEpochCache Optionally a cache for updating the leader epoch during recovery.
   * @return The number of bytes truncated from the log
   * @throws LogSegmentOffsetOverflowException if the log segment contains an offset that causes the index offset to overflow
   */
  @nonthreadsafe
  def recover(producerStateManager: ProducerStateManager, leaderEpochCache: Option[LeaderEpochFileCache] = None): Int = {
    // 1. reset all Index [删除索引中索引数据]
    offsetIndex.reset()
    timeIndex.reset()
    txnIndex.reset()
    var validBytes = 0
    var lastIndexEntry = 0
    maxTimestampAndOffsetSoFar = TimestampOffset.UNKNOWN
    try {
      // 2. verify all batchs in logsegment [遍历 取出FileChannelRecordBatch]
      for (batch <- log.batches.asScala) {
        // ** 2.1 Calculate the checksum to verify
        // 如果校验不过，会抛出异常 [ CorruptRecordException ]，就不会校验下一个RecordBatch了，会直接进入到truncate环节
        batch.ensureValid()

        // 位移值校验
        ensureOffsetInRange(batch.lastOffset)

        // The max timestamp is exposed at the batch level, so no need to iterate the records
        // 如果遍历的batch中某个batch的最大时间戳，大于维护的 [maxTimestampSoFar]，则更新
        if (batch.maxTimestamp > maxTimestampSoFar) {
          maxTimestampAndOffsetSoFar = new TimestampOffset(batch.maxTimestamp, batch.lastOffset)
        }

        // 2.2 Build offset index
        // validBytes - lastIndexEntry = "recover" 了多少数据
        // recover的数量大于 4096bytes就要插入索引了
        if (validBytes - lastIndexEntry > indexIntervalBytes) {
          offsetIndex.append(batch.lastOffset, validBytes)
          timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar)
          lastIndexEntry = validBytes
        }
        // 更新
        validBytes += batch.sizeInBytes()

        // v2版本以上的消息，还需处理 leaderEpochCache 和 producerStateManager
        if (batch.magic >= RecordBatch.MAGIC_VALUE_V2) {
          // recover的batch中存在 某个batch的partitionLeaderEpoch 比 leaderEpochCache中大的，则更新leaderEpochCache
          leaderEpochCache.foreach { cache =>
            if (batch.partitionLeaderEpoch >= 0 && cache.latestEpoch.asScala.forall(batch.partitionLeaderEpoch > _))
              cache.assign(batch.partitionLeaderEpoch, batch.baseOffset)
          }
          // 处理producerStateManager
          updateProducerState(producerStateManager, batch)
        }
      }
    } catch {
      case e@ (_: CorruptRecordException | _: InvalidRecordException) =>
        warn("Found invalid messages in log segment %s at byte offset %d: %s. %s"
          .format(log.file.getAbsolutePath, validBytes, e.getMessage, e.getCause))
    }

    // [log-truncate场景]
    // 遍历完了某个logsegment中所有RecordBatch之后的逻辑，截断逻辑
    val truncated = log.sizeInBytes - validBytes
    if (truncated > 0)
      debug(s"Truncated $truncated invalid bytes at the end of segment ${log.file.getAbsoluteFile} during recovery")

    // 3. truncate [log, index]
    log.truncateTo(validBytes)
    offsetIndex.trimToValidSize()
    // A normally closed segment always appends the biggest timestamp ever seen into log segment, we do this as well.
    timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar, true)
    timeIndex.trimToValidSize()
    truncated
  }

  private def loadLargestTimestamp(): Unit = {
    // Get the last time index entry. If the time index is empty, it will return (-1, baseOffset)
    val lastTimeIndexEntry = timeIndex.lastEntry
    maxTimestampAndOffsetSoFar = lastTimeIndexEntry

    val offsetPosition = offsetIndex.lookup(lastTimeIndexEntry.offset)
    // Scan the rest of the messages to see if there is a larger timestamp after the last time index entry.
    val maxTimestampOffsetAfterLastEntry = log.largestTimestampAfter(offsetPosition.position)
    if (maxTimestampOffsetAfterLastEntry.timestamp > lastTimeIndexEntry.timestamp) {
      maxTimestampAndOffsetSoFar = new TimestampOffset(maxTimestampOffsetAfterLastEntry.timestamp, maxTimestampOffsetAfterLastEntry.offset)
    }
  }

  /**
   * Check whether the last offset of the last batch in this segment overflows the indexes.
   */
  def hasOverflow: Boolean = {
    val nextOffset = readNextOffset
    nextOffset > baseOffset && !canConvertToRelativeOffset(nextOffset - 1)
  }

  def collectAbortedTxns(fetchOffset: Long, upperBoundOffset: Long): TxnIndexSearchResult =
    txnIndex.collectAbortedTxns(fetchOffset, upperBoundOffset)

  override def toString: String = "LogSegment(baseOffset=" + baseOffset +
    ", size=" + size +
    ", lastModifiedTime=" + lastModified +
    ", largestRecordTimestamp=" + largestRecordTimestamp +
    ")"

  /**
   * Truncate off all index and log entries with offsets >= the given offset.
   * If the given offset is larger than the largest message in this segment, do nothing.
   *
   * @param offset The offset to truncate to
   * @return The number of log bytes truncated
   */
  @nonthreadsafe
  def truncateTo(offset: Long): Int = {
    // Do offset translation before truncating the index to avoid needless scanning
    // in case we truncate the full index
    // 找到你要截取的位置offset的position，处于第多少个Byte位置
    val mapping = translateOffset(offset)

    // index truncate
    offsetIndex.truncateTo(offset)
    timeIndex.truncateTo(offset)
    txnIndex.truncateTo(offset)

    // After truncation, reset and allocate more space for the (new currently active) index
    offsetIndex.resize(offsetIndex.maxIndexSize)
    timeIndex.resize(timeIndex.maxIndexSize)

    // log truncate： 调用JDK中FileChannel的原生truncate能力
    val bytesTruncated = if (mapping == null) 0 else log.truncateTo(mapping.position)

    if (log.sizeInBytes == 0) {
      created = time.milliseconds
      rollingBasedTimestamp = None
    }

    bytesSinceLastIndexEntry = 0
    // 更新维护的maxTimestampAndOffsetSoFar，因为日志和索引都经过了truncate，原来的maxTimestampAndOffsetSoFar过期了
    if (maxTimestampSoFar >= 0)
      loadLargestTimestamp()

    // 返回截取了多少
    bytesTruncated
  }

  /**
   * Calculate the offset that would be used for the next message to be append to this segment.
   * Note that this is expensive.
   */
  @threadsafe
  def readNextOffset: Long = {
    val fetchData = read(offsetIndex.lastOffset, log.sizeInBytes)
    if (fetchData == null)
      baseOffset
    else
      fetchData.records.batches.asScala.lastOption
        .map(_.nextOffset)
        .getOrElse(baseOffset)
  }

  /**
   * Flush this log segment to disk
   * Forces any changes made to this buffer's content to be written to the storage device
   */
  @threadsafe
  def flush(): Unit = {
    LogFlushStats.logFlushTimer.time { () =>
      log.flush()
      offsetIndex.flush()
      timeIndex.flush()
      txnIndex.flush()
      // 都是让JDK调操作系统的能力
    }
  }

  /**
   * Update the directory reference for the log and indices in this segment. This would typically be called after a
   * directory is renamed.
   */
  def updateParentDir(dir: File): Unit = {
    log.updateParentDir(dir)
    lazyOffsetIndex.updateParentDir(dir)
    lazyTimeIndex.updateParentDir(dir)
    txnIndex.updateParentDir(dir)
  }

  /**
   * Change the suffix for the index and log files for this log segment
   * IOException from this method should be handled by the caller
   */
  def changeFileSuffixes(oldSuffix: String, newSuffix: String): Unit = {
    log.renameTo(new File(Utils.replaceSuffix(log.file.getPath, oldSuffix, newSuffix)))
    lazyOffsetIndex.renameTo(new File(Utils.replaceSuffix(lazyOffsetIndex.file.getPath, oldSuffix, newSuffix)))
    lazyTimeIndex.renameTo(new File(Utils.replaceSuffix(lazyTimeIndex.file.getPath, oldSuffix, newSuffix)))
    txnIndex.renameTo(new File(Utils.replaceSuffix(txnIndex.file.getPath, oldSuffix, newSuffix)))
  }

  def hasSuffix(suffix: String): Boolean = {
    log.file.getName.endsWith(suffix) &&
    lazyOffsetIndex.file.getName.endsWith(suffix) &&
    lazyTimeIndex.file.getName.endsWith(suffix) &&
    txnIndex.file.getName.endsWith(suffix)
  }

  /**
   * Append the largest time index entry to the time index and trim the log and indexes.
   *
   * The time index entry appended will be used to decide when to delete the segment.
   */
  def onBecomeInactiveSegment(): Unit = {
    timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar, true)
    offsetIndex.trimToValidSize()
    timeIndex.trimToValidSize()
    log.trim()
  }

  /**
    * If not previously loaded,
    * load the timestamp of the first message into memory.
    */
  private def loadFirstBatchTimestamp(): Unit = {
    if (rollingBasedTimestamp.isEmpty) {
      val iter = log.batches.iterator()
      if (iter.hasNext)
        rollingBasedTimestamp = Some(iter.next().maxTimestamp)
    }
  }

  /**
   * The time this segment has waited to be rolled.
   * If the first message batch has a timestamp we use its timestamp to determine when to roll a segment. A segment
   * is rolled if the difference between the new batch's timestamp and the first batch's timestamp exceeds the
   * segment rolling time.
   * If the first batch does not have a timestamp, we use the wall clock time to determine when to roll a segment. A
   * segment is rolled if the difference between the current wall clock time and the segment create time exceeds the
   * segment rolling time.
   */

  /**
   * @param now 当前时间
   * @param messageTimestamp 想要往此LogSegment中添加的Records中的最大Timestamp
   * @return
   */
  def timeWaitedForRoll(now: Long, messageTimestamp: Long): Long = {
    // Load the timestamp of the first message into memory
    // 1. 现在LogSegment第一条消息的Timestamp
    loadFirstBatchTimestamp()

    rollingBasedTimestamp match {
      // rollingBasedTimestamp存在的情况：Record间的Timestamp差距
      case Some(t) if t >= 0 => messageTimestamp - t
      // rollingBasedTimestamp不存在的情况：now 与 logsegment创建时的Timestamp差距
      case _ => now - created
    }
  }

  /**
    * @return the first batch timestamp if the timestamp is available. Otherwise return Long.MaxValue
    */
  def getFirstBatchTimestamp(): Long = {
    loadFirstBatchTimestamp()
    rollingBasedTimestamp match {
      case Some(t) if t >= 0 => t
      case _ => Long.MaxValue
    }
  }

  /**
   * Search the message offset based on timestamp and offset.
   *
   * This method returns an option of TimestampOffset. The returned value is determined using the following ordered list of rules:
   *
   * - If all the messages in the segment have smaller offsets, return None
   * - If all the messages in the segment have smaller timestamps, return None
   * - If all the messages in the segment have larger timestamps, or no message in the segment has a timestamp
   *   the returned the offset will be max(the base offset of the segment, startingOffset) and the timestamp will be Message.NoTimestamp.
   * - Otherwise, return an option of TimestampOffset. The offset is the offset of the first message whose timestamp
   *   is greater than or equals to the target timestamp and whose offset is greater than or equals to the startingOffset.
   *
   * This methods only returns None when 1) all messages' offset < startOffing or 2) the log is not empty but we did not
   * see any message when scanning the log from the indexed position. The latter could happen if the log is truncated
   * after we get the indexed position but before we scan the log from there. In this case we simply return None and the
   * caller will need to check on the truncated log and maybe retry or even do the search on another log segment.
   *
   * @param timestamp The timestamp to search for.
   * @param startingOffset The starting offset to search.
   * @return the timestamp and offset of the first message that meets the requirements. None will be returned if there is no such message.
   */
  def findOffsetByTimestamp(timestamp: Long, startingOffset: Long = baseOffset): Option[TimestampAndOffset] = {
    // Get the index entry with a timestamp less than or equal to the target timestamp
    val timestampOffset = timeIndex.lookup(timestamp)
    val position = offsetIndex.lookup(math.max(timestampOffset.offset, startingOffset)).position

    // Search the timestamp
    Option(log.searchForTimestamp(timestamp, position, startingOffset))
  }

  /**
   * Close this log segment
   */
  def close(): Unit = {
    if (_maxTimestampAndOffsetSoFar != TimestampOffset.UNKNOWN)
      CoreUtils.swallow(timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar, true), this)
    CoreUtils.swallow(lazyOffsetIndex.close(), this)
    CoreUtils.swallow(lazyTimeIndex.close(), this)
    CoreUtils.swallow(log.close(), this)
    CoreUtils.swallow(txnIndex.close(), this)
  }

  /**
    * Close file handlers used by the log segment but don't write to disk. This is used when the disk may have failed
    */
  def closeHandlers(): Unit = {
    CoreUtils.swallow(lazyOffsetIndex.closeHandler(), this)
    CoreUtils.swallow(lazyTimeIndex.closeHandler(), this)
    CoreUtils.swallow(log.closeHandlers(), this)
    CoreUtils.swallow(txnIndex.close(), this)
  }

  /**
   * Delete this log segment from the filesystem.
   */
  def deleteIfExists(): Unit = {
    def delete(delete: () => Boolean, fileType: String, file: File, logIfMissing: Boolean): Unit = {
      try {
        if (delete())
          info(s"Deleted $fileType ${file.getAbsolutePath}.")
        else if (logIfMissing)
          info(s"Failed to delete $fileType ${file.getAbsolutePath} because it does not exist.")
      }
      catch {
        case e: IOException => throw new IOException(s"Delete of $fileType ${file.getAbsolutePath} failed.", e)
      }
    }

    CoreUtils.tryAll(Seq(
      () => delete(log.deleteIfExists _, "log", log.file, logIfMissing = true),
      () => delete(lazyOffsetIndex.deleteIfExists _, "offset index", lazyOffsetIndex.file, logIfMissing = true),
      () => delete(lazyTimeIndex.deleteIfExists _, "time index", lazyTimeIndex.file, logIfMissing = true),
      () => delete(txnIndex.deleteIfExists _, "transaction index", txnIndex.file, logIfMissing = false)
    ))
  }

  def deleted(): Boolean = {
    !log.file.exists() && !lazyOffsetIndex.file.exists() && !lazyTimeIndex.file.exists() && !txnIndex.file.exists()
  }

  /**
   * The last modified time of this log segment as a unix time stamp
   */
  def lastModified = log.file.lastModified

  /**
   * The largest timestamp this segment contains, if maxTimestampSoFar >= 0, otherwise None.
   */
  def largestRecordTimestamp: Option[Long] = if (maxTimestampSoFar >= 0) Some(maxTimestampSoFar) else None

  /**
   * The largest timestamp this segment contains.
   */
  def largestTimestamp = if (maxTimestampSoFar >= 0) maxTimestampSoFar else lastModified

  /**
   * Change the last modified time for this log segment
   */
  def lastModified_=(ms: Long) = {
    val fileTime = FileTime.fromMillis(ms)
    Files.setLastModifiedTime(log.file.toPath, fileTime)
    Files.setLastModifiedTime(lazyOffsetIndex.file.toPath, fileTime)
    Files.setLastModifiedTime(lazyTimeIndex.file.toPath, fileTime)
  }

}

object LogSegment {

  def open(dir: File, baseOffset: Long, config: LogConfig, time: Time, fileAlreadyExists: Boolean = false,
           initFileSize: Int = 0, preallocate: Boolean = false, fileSuffix: String = ""): LogSegment = {
    val maxIndexSize = config.maxIndexSize

//    * @param log The file records containing log entries | FileRecords
//    * @param lazyOffsetIndex The offset index
//    * @param lazyTimeIndex The timestamp index
//    * @param txnIndex The transaction index
//    * @param baseOffset A lower bound on the offsets in this segment
//    * @param indexIntervalBytes The approximate number of bytes between entries in the index
//    * @param rollJitterMs The maximum random jitter subtracted from the scheduled segment roll time
//    * @param time The time instance

    new LogSegment(
      FileRecords.open(UnifiedLog.logFile(dir, baseOffset, fileSuffix), fileAlreadyExists, initFileSize, preallocate),
      LazyIndex.forOffset(UnifiedLog.offsetIndexFile(dir, baseOffset, fileSuffix), baseOffset, maxIndexSize),
      LazyIndex.forTime(UnifiedLog.timeIndexFile(dir, baseOffset, fileSuffix), baseOffset, maxIndexSize),
      new TransactionIndex(baseOffset, UnifiedLog.transactionIndexFile(dir, baseOffset, fileSuffix)),
      baseOffset,
      indexIntervalBytes = config.indexInterval,
      rollJitterMs = config.randomSegmentJitter,
      time)
  }

  def deleteIfExists(dir: File, baseOffset: Long, fileSuffix: String = ""): Unit = {
    UnifiedLog.deleteFileIfExists(UnifiedLog.offsetIndexFile(dir, baseOffset, fileSuffix))
    UnifiedLog.deleteFileIfExists(UnifiedLog.timeIndexFile(dir, baseOffset, fileSuffix))
    UnifiedLog.deleteFileIfExists(UnifiedLog.transactionIndexFile(dir, baseOffset, fileSuffix))
    UnifiedLog.deleteFileIfExists(UnifiedLog.logFile(dir, baseOffset, fileSuffix))
  }
}

/**
 * 日志刷盘指标监控
 */
object LogFlushStats {
  private val metricsGroup = new KafkaMetricsGroup(LogFlushStats.getClass)
  val logFlushTimer: Timer = metricsGroup.newTimer("LogFlushRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS)
}
