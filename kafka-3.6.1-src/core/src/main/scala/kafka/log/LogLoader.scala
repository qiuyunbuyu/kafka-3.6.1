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

import java.io.{File, IOException}
import java.nio.file.{Files, NoSuchFileException}
import kafka.common.LogSegmentOffsetOverflowException
import kafka.log.UnifiedLog.{CleanedFileSuffix, SwapFileSuffix, isIndexFile, isLogFile, offsetFromFile}
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InvalidOffsetException
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.snapshot.Snapshots
import org.apache.kafka.server.util.Scheduler
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache
import org.apache.kafka.storage.internals.log.{CorruptIndexException, LoadedLogOffsets, LogConfig, LogDirFailureChannel, LogFileUtils, LogOffsetMetadata, ProducerStateManager}

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import scala.collection.{Set, mutable}
import scala.jdk.CollectionConverters._

object LogLoader extends Logging {

  /**
   * Clean shutdown file that indicates the broker was cleanly shutdown in 0.8 and higher.
   * This is used to avoid unnecessary recovery after a clean shutdown. In theory this could be
   * avoided by passing in the recovery point, however finding the correct position to do this
   * requires accessing the offset index which may not be safe in an unclean shutdown.
   * For more information see the discussion in PR#2104
   */
  val CleanShutdownFile = ".kafka_cleanshutdown"
}


/**
 * @param dir The directory from which log segments need to be loaded
 * @param topicPartition The topic partition associated with the log being loaded
 * @param config The configuration settings for the log being loaded
 * @param scheduler The thread pool scheduler used for background actions
 * @param time The time instance used for checking the clock
 * @param logDirFailureChannel The LogDirFailureChannel instance to asynchronously handle log
 *                             directory failure
 * @param hadCleanShutdown Boolean flag to indicate whether the associated log previously had a
 *                         clean shutdown
 * @param segments The LogSegments instance into which segments recovered from disk will be
 *                 populated
 * @param logStartOffsetCheckpoint The checkpoint of the log start offset
 * @param recoveryPointCheckpoint The checkpoint of the offset at which to begin the recovery
 * @param leaderEpochCache An optional LeaderEpochFileCache instance to be updated during recovery
 * @param producerStateManager The ProducerStateManager instance to be updated during recovery
 * @param numRemainingSegments The remaining segments to be recovered in this log keyed by recovery thread name
 */
class LogLoader(
  dir: File,
  topicPartition: TopicPartition,
  config: LogConfig,
  scheduler: Scheduler,
  time: Time,
  logDirFailureChannel: LogDirFailureChannel,
  hadCleanShutdown: Boolean,
  segments: LogSegments,
  logStartOffsetCheckpoint: Long,
  recoveryPointCheckpoint: Long,
  leaderEpochCache: Option[LeaderEpochFileCache],
  producerStateManager: ProducerStateManager,
  numRemainingSegments: ConcurrentMap[String, Int] = new ConcurrentHashMap[String, Int],
  isRemoteLogEnabled: Boolean = false,
) extends Logging {
  logIdent = s"[LogLoader partition=$topicPartition, dir=${dir.getParent}] "

  /**
   * Load the log segments from the log files on disk, and returns the components of the loaded log.
   * Additionally, it also suitably updates the provided LeaderEpochFileCache and ProducerStateManager
   * to reflect the contents of the loaded log.
   *
   * In the context of the calling thread, this function does not need to convert IOException to
   * KafkaStorageException because it is only called before all logs are loaded.
   *
   * @return the offsets of the Log successfully loaded from disk
   *
   * @throws LogSegmentOffsetOverflowException if we encounter a .swap file with messages that
   *                                           overflow index offset
   *
   * 目标是什么：
   * 目标1：返回LoadedLogOffsets对象，包含下面的描述Log的信息 logStartOffset + recoveryPoint + nextOffsetMetadata
   * 目标2：把跳表map对象，即segments: LogSegments, 构造起来
   * 目标3：更新LeaderEpochFileCache
   * 目标4：更新ProducerStateManager
   */
  def load(): LoadedLogOffsets = {
    // ========================================= 环节1： 处理临时文件.swap .clean .delete 文件 =========================
    // [ First pass ]: through the files in the log directory and remove any temporary files
    // and find any interrupted swap operations
    // Clean up files with .clean and .delete suffixes, and keep files with valid .swap suffixes
    val swapFiles = removeTempFilesAndCollectSwapFiles()

    // The remaining valid swap files must come from compaction or segment split operation. We can
    // simply rename them to regular segment files. But, before renaming, we should figure out which
    // segments are compacted/split and delete these segment files: this is done by calculating
    // min/maxSwapFileOffset.
    // We store segments that require renaming in this code block, and do the actual renaming later.
    var minSwapFileOffset = Long.MaxValue
    var maxSwapFileOffset = Long.MinValue
    swapFiles.filter(f => UnifiedLog.isLogFile(new File(Utils.replaceSuffix(f.getPath, SwapFileSuffix, "")))).foreach { f =>
      // 从文件名获取baseOffset
      val baseOffset = offsetFromFile(f)
      // 从.swap文件中构建出一个 .swap结尾的 logsegment， 这步能有数据吗？ ↓
      // 在完成对日志数据的压缩操作后，会将压缩的结果先保存为 swap 文件（以“.swap”作为文件后缀），并最终替换压缩前的日志文件，所以 swap 文件中的数据都是完整，只需要移除对应的“.swap”后缀，并构建对应的 LogSegment 对象即可
      val segment = LogSegment.open(f.getParentFile,
        baseOffset = baseOffset,
        config,
        time = time,
        fileSuffix = UnifiedLog.SwapFileSuffix)
      info(s"Found log file ${f.getPath} from interrupted swap operation, which is recoverable from ${UnifiedLog.SwapFileSuffix} files by renaming.")
      // 遍历所有的.swap文件，构建出一个[minSwapFileOffset, maxSwapFileOffset]区间给下一步使用
      minSwapFileOffset = Math.min(segment.baseOffset, minSwapFileOffset)
      maxSwapFileOffset = Math.max(segment.readNextOffset, maxSwapFileOffset)
    }

    // [ Second pass ]: delete segments that are between minSwapFileOffset and maxSwapFileOffset. As
    // discussed above, these segments were compacted or split but haven't been renamed to .delete
    // before shutting down the broker.
    for (file <- dir.listFiles if file.isFile) {
      try {
        if (!file.getName.endsWith(SwapFileSuffix)) {
          val offset = offsetFromFile(file) // baseoffset处于[minSwapFileOffset, maxSwapFileOffset]的被认为是”is compacted but has not been deleted yet“，所以可以直接删掉了
          if (offset >= minSwapFileOffset && offset < maxSwapFileOffset) {
            info(s"Deleting segment files ${file.getName} that is compacted but has not been deleted yet.")
            file.delete()
          }
        }
      } catch {
        // offsetFromFile with files that do not include an offset in the file name
        case _: StringIndexOutOfBoundsException =>
        case _: NumberFormatException =>
      }
    }

    // [ Third pass ]: rename all swap files.
    // 前面有"从.swap文件中构建出一个 .swap结尾的 LogSegment"的步骤, 此时可删除 ”.swap结尾“，临时swap结尾的LogSegment转正了
    for (file <- dir.listFiles if file.isFile) {
      if (file.getName.endsWith(SwapFileSuffix)) {
        info(s"Recovering file ${file.getName} by renaming from ${UnifiedLog.SwapFileSuffix} files.")
        file.renameTo(new File(Utils.replaceSuffix(file.getPath, UnifiedLog.SwapFileSuffix, "")))
      }
    }

    // ========================================= 环节2： LogSegments对象处理 =========================
    // [ Fourth pass ]: load all the log and index files.
    // We might encounter legacy log segments with offset overflow (KAFKA-6264). We need to split such segments. When
    // this happens, restart loading segment files from scratch.
    retryOnOffsetOverflow(() => {
      // In case we encounter a segment with offset overflow, the retry logic will split it after which we need to retry
      // loading of segments. In that case, we also need to close all segments that could have been left open in previous
      // call to loadSegmentFiles().
      segments.close()
      // 清空原Map对象
      segments.clear()
      // * Loads segments from disk into the provided params.segments.
      loadSegmentFiles()
    })

    // “a tuple containing ( [newRecoveryPoint] + [nextOffset] ).”
    val (newRecoveryPoint: Long, nextOffset: Long) = {
      // case1: a directory that is scheduled to be deleted, skip it
      // case2: a normal directory that should check "Need to check whether recovery is needed"
      if (!dir.getAbsolutePath.endsWith(UnifiedLog.DeleteDirSuffix)) {

        // 执行recoverLog，会判断是否unclean shutdown
        // "Recover the log segments (if there was an unclean shutdown)"
        val (newRecoveryPoint, nextOffset) = retryOnOffsetOverflow(recoverLog)

        // reset the index size of the currently active log segment to allow more entries
        segments.lastSegment.get.resizeIndexes(config.maxIndexSize)
        (newRecoveryPoint, nextOffset)
      } else {
      // case3: If no segment exists, create the first segment
        if (segments.isEmpty) {
          // will add First LogSegment to "LogSegments"
          segments.add(
            LogSegment.open(
              dir = dir,
              baseOffset = 0,
              config,
              time = time,
              initFileSize = config.initFileSize))
        }
        (0L, 0L)
      }
    }

    leaderEpochCache.foreach(_.truncateFromEnd(nextOffset))

    // [ logStartOffset ]
    val newLogStartOffset = if (isRemoteLogEnabled) {
      logStartOffsetCheckpoint
    } else {
      math.max(logStartOffsetCheckpoint, segments.firstSegment.get.baseOffset)
    }
    // The earliest leader epoch may not be flushed during a hard failure. Recover it here.
    leaderEpochCache.foreach(_.truncateFromStart(logStartOffsetCheckpoint))

    // Any segment loading or recovery code must not use producerStateManager, so that we can build the full state here
    // from scratch.
    if (!producerStateManager.isEmpty)
      throw new IllegalStateException("Producer state must be empty during log initialization")

    // Reload all snapshots into the ProducerStateManager cache, the intermediate ProducerStateManager used
    // during log recovery may have deleted some files without the LogLoader.producerStateManager instance witnessing the
    // deletion.
    producerStateManager.removeStraySnapshots(segments.baseOffsets.map(x => Long.box(x)).asJavaCollection)
    UnifiedLog.rebuildProducerState(
      producerStateManager,
      segments,
      newLogStartOffset,
      nextOffset,
      config.recordVersion,
      time,
      reloadFromCleanShutdown = hadCleanShutdown,
      logIdent)
    val activeSegment = segments.lastSegment.get

    // 返回 LoadedLogOffsets
    new LoadedLogOffsets(
      newLogStartOffset, // [ logStartOffset ]
      newRecoveryPoint, // [newRecoveryPoint]
      new LogOffsetMetadata(nextOffset, activeSegment.baseOffset, activeSegment.size)) // nextOffset -> logEndOffset
  }

  /**
   * Removes any temporary files found in log directory, and creates a list of all .swap files which could be swapped
   * in place of existing segment(s). For log splitting, we know that any .swap file whose base offset is higher than
   * the smallest offset .clean file could be part of an incomplete split operation. Such .swap files are also deleted
   * by this method.
   *
   * 就干了2个活：
   * 删除了 (invalid .swap .clean .delete)
   * 返回了 (valid .swap)
   *
   * 要想看懂这个，还是得懂 .swap .clean .delete 文件的生命周期以及作用
   * @return Set of .swap files that are valid to be swapped in as segment files and index files
   */
  private def removeTempFilesAndCollectSwapFiles(): Set[File] = {
    // Set to store swapFiles & cleanedFiles
    // 收集 .swap files
    val swapFiles = mutable.Set[File]()
    // 收集 .clean files
    val cleanedFiles = mutable.Set[File]()
    // .clean files中 最小的 offset
    var minCleanedFileOffset = Long.MaxValue

    for (file <- dir.listFiles if file.isFile) {
      if (!file.canRead)
        throw new IOException(s"Could not read file $file")
      val filename = file.getName

      // Delete stray files marked for deletion, but skip KRaft snapshots.
      // These are handled in the recovery logic in `KafkaMetadataLog`.
      if (filename.endsWith(LogFileUtils.DELETED_FILE_SUFFIX) && !filename.endsWith(Snapshots.DELETE_SUFFIX)) {
        // case1: Delete all files ending with .delete except .checkpoint.delete
        debug(s"Deleting stray temporary file ${file.getAbsolutePath}")
        Files.deleteIfExists(file.toPath) // 删除.checkpoint.delete之外的所有.delete结尾的文件
      } else if (filename.endsWith(CleanedFileSuffix)) {
        // case2: store cleanedFiles[.cleaned] and update minCleanedFileOffset
        minCleanedFileOffset = Math.min(offsetFromFile(file), minCleanedFileOffset)
        cleanedFiles += file // 保存.cleaned结尾的文件
      } else if (filename.endsWith(SwapFileSuffix)) {
        // case3:  store swapFiles[.swap]
        swapFiles += file // 保存.swap结尾的文件
      }
    }

    // 删除invalid的 .swap文件
    // KAFKA-6264: Delete all .swap files whose base offset is greater than the minimum .cleaned segment offset. Such .swap
    // files could be part of an incomplete split operation that could not complete. See Log#splitOverflowedSegment
    // for more details about the split operation.
    val (invalidSwapFiles, validSwapFiles) = swapFiles.partition(file => offsetFromFile(file) >= minCleanedFileOffset)
    // delete invalidSwapFiles
    invalidSwapFiles.foreach { file =>
      debug(s"Deleting invalid swap file ${file.getAbsoluteFile} minCleanedFileOffset: $minCleanedFileOffset")
      Files.deleteIfExists(file.toPath)
    }

    // 删除.cleaned结尾的文件
    // delete cleanedFiles
    // Now that we have deleted all .swap files that constitute an incomplete split operation, let's delete all .clean files
    cleanedFiles.foreach { file =>
      debug(s"Deleting stray .clean file ${file.getAbsolutePath}")
      Files.deleteIfExists(file.toPath)
    }

    // return validSwapFiles
    // 返回valid的.swap文件
    validSwapFiles
  }

  /**
   * Retries the provided function only whenever an LogSegmentOffsetOverflowException is raised by
   * it during execution. Before every retry, the overflowed segment is split into one or more segments
   * such that there is no offset overflow in any of them.
   *
   * @param fn The function to be executed
   * @return The value returned by the function, if successful
   * @throws Exception whenever the executed function throws any exception other than
   *                   LogSegmentOffsetOverflowException, the same exception is raised to the caller
   */
  private def retryOnOffsetOverflow[T](fn: () => T): T = {
    while (true) {
      try {
        return fn()
      } catch {
        case e: LogSegmentOffsetOverflowException =>
          info(s"Caught segment overflow error: ${e.getMessage}. Split segment and retry.")
          val result = UnifiedLog.splitOverflowedSegment(
            e.segment,
            segments,
            dir,
            topicPartition,
            config,
            scheduler,
            logDirFailureChannel,
            logIdent)
          deleteProducerSnapshotsAsync(result.deletedSegments)
      }
    }
    throw new IllegalStateException()
  }

  /**
   * Loads segments from disk into the provided params.segments.
   *
   * This method does not need to convert IOException to KafkaStorageException because it is only called before all logs are loaded.
   * It is possible that we encounter a segment with index offset overflow in which case the LogSegmentOffsetOverflowException
   * will be thrown. Note that any segments that were opened before we encountered the exception will remain open and the
   * caller is responsible for closing them appropriately, if needed.
   *
   * @throws LogSegmentOffsetOverflowException if the log directory contains a segment with messages that overflow the index offset
   */
  private def loadSegmentFiles(): Unit = {
    // load segments in ascending order because transactional data from one segment may depend on the
    // segments that come before it
    // 遍历某个Topic-Partition-x目录下所以文件
    for (file <- dir.listFiles.sortBy(_.getName) if file.isFile) {
      // index索引文件
      if (isIndexFile(file)) {
        // 如果有索引文件，但没对应的log文件，则删除
        // if it is an index file, make sure it has a corresponding .log file
        val offset = offsetFromFile(file)
        val logFile = UnifiedLog.logFile(dir, offset)
        if (!logFile.exists) {
          warn(s"Found an orphaned index file ${file.getAbsolutePath}, with no corresponding log file.")
          Files.deleteIfExists(file.toPath)
        }
      } else if (isLogFile(file)) {
        // 处理log文件
        // if it's a log file, load the corresponding log segment
        // 获取baseoffset
        val baseOffset = offsetFromFile(file)
        // 判断timeindex文件是否存在
        val timeIndexFileNewlyCreated = !UnifiedLog.timeIndexFile(dir, baseOffset).exists()
        // 创建了xxxx.log文件(如果不存在) + 拿到了能操作这个logsegment中相关文件的channel
        val segment = LogSegment.open(
          dir = dir,
          baseOffset = baseOffset,
          config,
          time = time,
          fileAlreadyExists = true)

        // 对index进行校验，出现异常，会进入recoverSegment逻辑
        try segment.sanityCheck(timeIndexFileNewlyCreated)
        catch {
          case _: NoSuchFileException =>
            if (hadCleanShutdown || segment.baseOffset < recoveryPointCheckpoint)
              error(s"Could not find offset index file corresponding to log file" +
                s" ${segment.log.file.getAbsolutePath}, recovering segment and rebuilding index files...")
            recoverSegment(segment)
          case e: CorruptIndexException =>
            warn(s"Found a corrupted index file corresponding to log file" +
              s" ${segment.log.file.getAbsolutePath} due to ${e.getMessage}}, recovering segment and" +
              " rebuilding index files...")
            recoverSegment(segment)
        }
        // 将此LogSegment加入[segments: ConcurrentNavigableMap[Long, LogSegment]]中
        segments.add(segment)
      }
    }
  }

  /**
   * Just recovers the given segment, without adding it to the provided params.segments.
   *
   * @param segment Segment to recover
   *
   * @return The number of bytes truncated from the segment
   *
   * @throws LogSegmentOffsetOverflowException if the segment contains messages that cause index offset overflow
   */
  private def recoverSegment(segment: LogSegment): Int = {
    val producerStateManager = new ProducerStateManager(
      topicPartition,
      dir,
      this.producerStateManager.maxTransactionTimeoutMs(),
      this.producerStateManager.producerStateManagerConfig(),
      time)
    UnifiedLog.rebuildProducerState(
      producerStateManager,
      segments,
      logStartOffsetCheckpoint,
      segment.baseOffset,
      config.recordVersion,
      time,
      reloadFromCleanShutdown = false,
      logIdent)
    // Run recovery on the given segment
    val bytesTruncated = segment.recover(producerStateManager, leaderEpochCache)
    // once we have recovered the segment's data, take a snapshot to ensure that we won't
    // need to reload the same segment again while recovering another segment.
    producerStateManager.takeSnapshot()
    bytesTruncated
  }

  /**
   * Recover the log segments (if there was an unclean shutdown). Ensures there is at least one
   * active segment, and returns the updated recovery point and next offset after recovery. Along
   * the way, the method suitably updates the LeaderEpochFileCache or ProducerStateManager inside
   * the provided LogComponents.
   *
   * This method does not need to convert IOException to KafkaStorageException because it is only
   * called before all logs are loaded.
   *
   * @return a tuple containing (newRecoveryPoint, nextOffset(logEndOffset)).
   *
   * @throws LogSegmentOffsetOverflowException if we encountered a legacy segment with offset overflow
   */
  private[log] def recoverLog(): (Long, Long) = {
    /** return the log end offset if valid */
    def deleteSegmentsIfLogStartGreaterThanLogEnd(): Option[Long] = {
      if (segments.nonEmpty) {
        val logEndOffset = segments.lastSegment.get.readNextOffset
        if (logEndOffset >= logStartOffsetCheckpoint)
          Some(logEndOffset)
        else {
          warn(s"Deleting all segments because logEndOffset ($logEndOffset) " +
            s"is smaller than logStartOffset $logStartOffsetCheckpoint. " +
            "This could happen if segment files were deleted from the file system.")
          removeAndDeleteSegmentsAsync(segments.values)
          leaderEpochCache.foreach(_.clearAndFlush())
          producerStateManager.truncateFullyAndStartAt(logStartOffsetCheckpoint)
          None
        }
      } else None
    }

    // If we have the clean shutdown marker, skip recovery.
    if (!hadCleanShutdown) {
      val unflushed = segments.values(recoveryPointCheckpoint, Long.MaxValue)
      val numUnflushed = unflushed.size
      val unflushedIter = unflushed.iterator
      var truncated = false
      var numFlushed = 0
      val threadName = Thread.currentThread().getName
      numRemainingSegments.put(threadName, numUnflushed)

      while (unflushedIter.hasNext && !truncated) {
        // ** only recover logSegments after recoveryPointCheckpoint, not all logSegments **
        val segment = unflushedIter.next()
        info(s"Recovering unflushed segment ${segment.baseOffset}. $numFlushed/$numUnflushed recovered for $topicPartition.")

        val truncatedBytes =
          try {
            // do recover
            recoverSegment(segment)
          } catch {
            case _: InvalidOffsetException =>
              val startOffset = segment.baseOffset
              warn(s"Found invalid offset during recovery. Deleting the" +
                s" corrupt segment and creating an empty one with starting offset $startOffset")
              segment.truncateTo(startOffset)
          }
        if (truncatedBytes > 0) {
          // we had an invalid message, delete all remaining log
          warn(s"Corruption found in segment ${segment.baseOffset}," +
            s" truncating to offset ${segment.readNextOffset}")
          // 删除场景：
          // unclean down场景下recoverSegment(segment) 时 truncatedBytes > 0
          // 证明[segment-x]出现下“异常”消息，那么处理流程如下
          // 1. [segment-x] 执行 truncate
          // 2. [segment-x +] 全部删除掉
          removeAndDeleteSegmentsAsync(unflushedIter.toList)
          truncated = true
          // segment is truncated, so set remaining segments to 0
          numRemainingSegments.put(threadName, 0)
        } else {
          numFlushed += 1
          numRemainingSegments.put(threadName, numUnflushed - numFlushed)
        }
      }
    }

    val logEndOffsetOption = deleteSegmentsIfLogStartGreaterThanLogEnd()

    if (segments.isEmpty) {
      // no existing segments, create a new mutable segment beginning at logStartOffset
      segments.add(
        LogSegment.open(
          dir = dir,
          baseOffset = logStartOffsetCheckpoint,
          config,
          time = time,
          initFileSize = config.initFileSize,
          preallocate = config.preallocate))
    }

    // Update the recovery point if there was a clean shutdown and did not perform any changes to
    // the segment. Otherwise, we just ensure that the recovery point is not ahead of the log end
    // offset. To ensure correctness and to make it easier to reason about, it's best to only advance
    // the recovery point when the log is flushed. If we advanced the recovery point here, we could
    // skip recovery for unflushed segments if the broker crashed after we checkpoint the recovery
    // point and before we flush the segment.
    // recover完，更新了一把(newRecoveryPoint, nextOffset)
    // 从这一步就可以知道即使手动把recovery Checkpoint 文件删除掉也没有关系，因为你不管是不是hadCleanShutdown，都需要走recover逻辑
    // 走完recover逻辑，对应的recoveryPointCheckpoint就会尝试更新至logEndOffset的地方

    // 但是如果是unclean shut down，并且recovery Checkpoint 文件也删掉了，就要付出巨大的“代价”，
    // 因为recovery Checkpoint没了，代表recoverpoint取出的值是0， 也就意味着每个LogSegment都要建立recover流程
    (hadCleanShutdown, logEndOffsetOption) match {
      case (true, Some(logEndOffset)) =>
        (logEndOffset, logEndOffset)
      case _ =>
        val logEndOffset = logEndOffsetOption.getOrElse(segments.lastSegment.get.readNextOffset)
        (Math.min(recoveryPointCheckpoint, logEndOffset), logEndOffset)
    }
  }

  /**
   * This method deletes the given log segments and the associated producer snapshots, by doing the
   * following for each of them:
   *  - It removes the segment from the segment map so that it will no longer be used for reads.
   *  - It schedules asynchronous deletion of the segments that allows reads to happen concurrently without
   *    synchronization and without the possibility of physically deleting a file while it is being
   *    read.
   *
   * This method does not need to convert IOException to KafkaStorageException because it is either
   * called before all logs are loaded or the immediate caller will catch and handle IOException
   *
   * @param segmentsToDelete The log segments to schedule for deletion
   */
  private def removeAndDeleteSegmentsAsync(segmentsToDelete: Iterable[LogSegment]): Unit = {
    if (segmentsToDelete.nonEmpty) {
      // Most callers hold an iterator into the `params.segments` collection and
      // `removeAndDeleteSegmentAsync` mutates it by removing the deleted segment. Therefore,
      // we should force materialization of the iterator here, so that results of the iteration
      // remain valid and deterministic. We should also pass only the materialized view of the
      // iterator to the logic that deletes the segments.
      val toDelete = segmentsToDelete.toList
      info(s"Deleting segments as part of log recovery: ${toDelete.mkString(",")}")
      toDelete.foreach { segment =>
        segments.remove(segment.baseOffset)
      }
      UnifiedLog.deleteSegmentFiles(
        toDelete,
        asyncDelete = true,
        dir,
        topicPartition,
        config,
        scheduler,
        logDirFailureChannel,
        logIdent)
      deleteProducerSnapshotsAsync(segmentsToDelete)
    }
  }

  private def deleteProducerSnapshotsAsync(segments: Iterable[LogSegment]): Unit = {
    UnifiedLog.deleteProducerSnapshots(segments,
      producerStateManager,
      asyncDelete = true,
      scheduler,
      config,
      logDirFailureChannel,
      dir.getParent,
      topicPartition)
  }
}
