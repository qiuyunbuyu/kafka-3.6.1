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

import java.io._
import java.nio.file.Files
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import kafka.server.checkpoints.OffsetCheckpointFile
import kafka.server.metadata.ConfigRepository
import kafka.server._
import kafka.utils._
import org.apache.kafka.common.{KafkaException, TopicPartition, Uuid}
import org.apache.kafka.common.utils.{KafkaThread, Time, Utils}
import org.apache.kafka.common.errors.{InconsistentTopicIdException, KafkaStorageException, LogDirNotFoundException}

import scala.jdk.CollectionConverters._
import scala.collection._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}
import kafka.utils.Implicits._
import org.apache.kafka.common.config.TopicConfig

import java.util.Properties
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.storage.internals.log.LogConfig.MessageFormatVersion
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.util.Scheduler
import org.apache.kafka.storage.internals.log.{CleanerConfig, LogConfig, LogDirFailureChannel, ProducerStateManagerConfig, RemoteIndexCache}

import scala.annotation.nowarn

/**
 * The entry point to the kafka log management subsystem. The log manager is responsible for log creation, retrieval, and cleaning.
 * All read and write operations are delegated to the individual log instances.
 *
 * The log manager maintains logs in one or more directories. New logs are created in the data directory
 * with the fewest logs. No attempt is made to move partitions after the fact or balance based on
 * size or I/O rate.
 *
 * A background thread handles log retention by periodically truncating excess log segments.
 */
@threadsafe
class LogManager(logDirs: Seq[File],
                 initialOfflineDirs: Seq[File],
                 configRepository: ConfigRepository,
                 val initialDefaultConfig: LogConfig,
                 val cleanerConfig: CleanerConfig,
                 recoveryThreadsPerDataDir: Int,
                 val flushCheckMs: Long,
                 val flushRecoveryOffsetCheckpointMs: Long,
                 val flushStartOffsetCheckpointMs: Long,
                 val retentionCheckMs: Long,
                 val maxTransactionTimeoutMs: Int,
                 val producerStateManagerConfig: ProducerStateManagerConfig,
                 val producerIdExpirationCheckIntervalMs: Int,
                 interBrokerProtocolVersion: MetadataVersion,
                 scheduler: Scheduler,
                 brokerTopicStats: BrokerTopicStats,
                 logDirFailureChannel: LogDirFailureChannel,
                 time: Time,
                 val keepPartitionMetadataFile: Boolean,
                 remoteStorageSystemEnable: Boolean) extends Logging {

  import LogManager._

  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  val InitialTaskDelayMs = 30 * 1000

  private val logCreationOrDeletionLock = new Object
  // key: TopicPartition 字符串组成的 TopicPartition标志
  // value: UnifiedLog主管某个TopicPartition
  private val currentLogs = new Pool[TopicPartition, UnifiedLog]()
  // Future logs are put in the directory with "-future" suffix. Future log is created when user wants to move replica
  // from one log directory to another log directory on the same broker. The directory of the future log will be renamed
  // to replace the current log of the partition after the future log catches up with the current log
  private val futureLogs = new Pool[TopicPartition, UnifiedLog]()

  // * Each element in the queue contains the log object to be deleted and the time it is scheduled for deletion.
  // independent thread: kafka-delete-logs | do deleteLogs() to delete the "Log files ending with .delete"
  private val logsToBeDeleted = new LinkedBlockingQueue[(UnifiedLog, Long)]()

  // Map of stray partition to stray log. This holds all stray logs detected on the broker.
  // Visible for testing
  private val strayLogs = new Pool[TopicPartition, UnifiedLog]()

  // 获取所有的liveLogDirs(排除异常情况，剩余的就是liveLogDirs) : 入参initialOfflineDirs是所有没读到meta.properties的dir
  private val _liveLogDirs: ConcurrentLinkedQueue[File] = createAndValidateLogDirs(logDirs, initialOfflineDirs)
  @volatile private var _currentDefaultConfig = initialDefaultConfig

  // num.recovery.threads.per.data.dir 为了完成loadlog的线程池中线程数量
  @volatile private var numRecoveryThreadsPerDataDir = recoveryThreadsPerDataDir

  // This map contains all partitions whose logs are getting loaded and initialized. If log configuration
  // of these partitions get updated at the same time, the corresponding entry in this map is set to "true",
  // which triggers a config reload after initialization is finished (to get the latest config value).
  // See KAFKA-8813 for more detail on the race condition
  // Visible for testing
  private[log] val partitionsInitializing = new ConcurrentHashMap[TopicPartition, Boolean]().asScala

  def reconfigureDefaultLogConfig(logConfig: LogConfig): Unit = {
    this._currentDefaultConfig = logConfig
  }

  def currentDefaultConfig: LogConfig = _currentDefaultConfig

  // Seq[File], /datax 存储目录下的所有文件 元数据(checkpoint系列)+文件夹(Topic-Partition-x)
  def liveLogDirs: Seq[File] = {
    if (_liveLogDirs.size == logDirs.size)
      logDirs
    else
      _liveLogDirs.asScala.toBuffer
  }
  // LogManager new 出来的时候，就要先获取liveLogDirs， 再尝试lock liveLogDirs
  private val dirLocks = lockLogDirs(liveLogDirs)
  @volatile private var recoveryPointCheckpoints = liveLogDirs.map(dir =>
    (dir, new OffsetCheckpointFile(new File(dir, RecoveryPointCheckpointFile), logDirFailureChannel))).toMap
  @volatile private var logStartOffsetCheckpoints = liveLogDirs.map(dir =>
    (dir, new OffsetCheckpointFile(new File(dir, LogStartOffsetCheckpointFile), logDirFailureChannel))).toMap

  private val preferredLogDirs = new ConcurrentHashMap[TopicPartition, String]()

  // logmanager中认为的offlineLogDirs， 只是【配置中设置的dir】 - 【_liveLogDirs】
  private def offlineLogDirs: Iterable[File] = {
    val logDirsSet = mutable.Set[File]() ++= logDirs
    _liveLogDirs.forEach(dir => logDirsSet -= dir)
    logDirsSet
  }

  // A map that stores hadCleanShutdown flag for each log dir.
  private val hadCleanShutdownFlags = new ConcurrentHashMap[String, Boolean]()

  // A map that tells whether all logs in a log dir had been loaded or not at startup time.
  private val loadLogsCompletedFlags = new ConcurrentHashMap[String, Boolean]()

  @volatile private var _cleaner: LogCleaner = _
  private[kafka] def cleaner: LogCleaner = _cleaner

  metricsGroup.newGauge("OfflineLogDirectoryCount", () => offlineLogDirs.size)

  for (dir <- logDirs) {
    metricsGroup.newGauge("LogDirectoryOffline",
      () => if (_liveLogDirs.contains(dir)) 0 else 1,
      Map("logDirectory" -> dir.getAbsolutePath).asJava)
  }

  /**
   * Create and check validity of the given directories that are not in the given offline directories, specifically:
   * <ol>
   * <li> Ensure that there are no duplicates in the directory list
   * <li> Create each directory if it doesn't exist
   * <li> Check that each path is a readable directory
   * </ol>
   */
  private def createAndValidateLogDirs(dirs: Seq[File], initialOfflineDirs: Seq[File]): ConcurrentLinkedQueue[File] = {
    val liveLogDirs = new ConcurrentLinkedQueue[File]()
    val canonicalPaths = mutable.HashSet.empty[String]

    for (dir <- dirs) {
      try {
        // 存在initialOfflineDirs中
        if (initialOfflineDirs.contains(dir))
          throw new IOException(s"Failed to load ${dir.getAbsolutePath} during broker startup")
        // 目录不存在
        if (!dir.exists) {
          info(s"Log directory ${dir.getAbsolutePath} not found, creating it.")
          //尝试创建
          val created = dir.mkdirs()
          if (!created)
            throw new IOException(s"Failed to create data directory ${dir.getAbsolutePath}")
          Utils.flushDir(dir.toPath.toAbsolutePath.normalize.getParent)
        }
        // 不可读
        if (!dir.isDirectory || !dir.canRead)
          throw new IOException(s"${dir.getAbsolutePath} is not a readable log directory.")
        // 重复
        // getCanonicalPath() throws IOException if a file system query fails or if the path is invalid (e.g. contains
        // the Nul character). Since there's no easy way to distinguish between the two cases, we treat them the same
        // and mark the log directory as offline.
        if (!canonicalPaths.add(dir.getCanonicalPath))
          throw new KafkaException(s"Duplicate log directory found: ${dirs.mkString(", ")}")

        // 排除异常情况，剩余的就是liveLogDirs
        liveLogDirs.add(dir)
      } catch {
        case e: IOException =>
          logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath, s"Failed to create or validate data directory ${dir.getAbsolutePath}", e)
      }
    }
    if (liveLogDirs.isEmpty) {
      fatal(s"Shutdown broker because none of the specified log dirs from ${dirs.mkString(", ")} can be created or validated")
      Exit.halt(1)
    }

    liveLogDirs
  }

  def resizeRecoveryThreadPool(newSize: Int): Unit = {
    info(s"Resizing recovery thread pool size for each data dir from $numRecoveryThreadsPerDataDir to $newSize")
    numRecoveryThreadsPerDataDir = newSize
  }

  /**
   * The log directory failure handler. It will stop log cleaning in that directory.
   *
   * @param dir        the absolute path of the log directory
   * 处理失败dir
   */
  def handleLogDirFailure(dir: String): Unit = {
    warn(s"Stopping serving logs in dir $dir")
    logCreationOrDeletionLock synchronized {
      // 从_liveLogDirs 移除此 目录
      _liveLogDirs.remove(new File(dir))

      if (_liveLogDirs.isEmpty) {
        fatal(s"Shutdown broker because all log dirs in ${logDirs.mkString(", ")} have failed")
        Exit.halt(1)
      }
      // recoveryPointCheckpoints集合中去除此offline dir
      recoveryPointCheckpoints = recoveryPointCheckpoints.filter { case (file, _) => file.getAbsolutePath != dir }
      // logStartOffsetCheckpoints集合中去除此offline dir
      logStartOffsetCheckpoints = logStartOffsetCheckpoints.filter { case (file, _) => file.getAbsolutePath != dir }

      // LogCleaner处理“Failure Dir”： ”Stopping cleaning logs in dir“
      if (cleaner != null)
        cleaner.handleLogDirFailure(dir)

      // *
      def removeOfflineLogs(logs: Pool[TopicPartition, UnifiedLog]): Iterable[TopicPartition] = {
        val offlineTopicPartitions: Iterable[TopicPartition] = logs.collect {
          case (tp, log) if log.parentDir == dir => tp
        }
        offlineTopicPartitions.foreach { topicPartition => {
          //  从logs: Pool[TopicPartition, UnifiedLog]移除offline tp
          val removedLog = removeLogAndMetrics(logs, topicPartition)
          removedLog.foreach {
          // "Close file handlers used by this log but don't write to disk. This is called if the log directory is offline"
            log => log.closeHandlers()
          }
        }}

        offlineTopicPartitions
      }

      // currentLogs和futureLogs 一起调用 removeOfflineLogs
      val offlineCurrentTopicPartitions = removeOfflineLogs(currentLogs)
      val offlineFutureTopicPartitions = removeOfflineLogs(futureLogs)

      warn(s"Logs for partitions ${offlineCurrentTopicPartitions.mkString(",")} are offline and " +
           s"logs for future partitions ${offlineFutureTopicPartitions.mkString(",")} are offline due to failure on log directory $dir")

      // dir.destroy(): Destroy this lock, closing the associated FileChannel
      dirLocks.filter(_.file.getParent == dir).foreach(dir => CoreUtils.swallow(dir.destroy(), this))
    }
  }

  /**
   * Lock all the given directories
   */
  private def lockLogDirs(dirs: Seq[File]): Seq[FileLock] = {
    dirs.flatMap { dir =>
      try {
        // 尝试lock
        val lock = new FileLock(new File(dir, LockFileName))
        if (!lock.tryLock())
          throw new KafkaException("Failed to acquire lock on file .lock in " + lock.file.getParent +
            ". A Kafka instance in another process or thread is using this directory.")
        Some(lock)
      } catch {
        case e: IOException =>
        // 因为.lock 权限不对，发生了IOException，所以加入了offlineLogDirQueue和offlineLogDirs
          logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath, s"Disk error while locking directory $dir", e)
          None
      }
    }
  }

  /**
   * 加入logsToBeDeleted队列的方法
   * 加入logsToBeDeleted队列场景1：LogManager启动时loadLog时发现存在.delete后缀的TopicPartition-x
   * 加入logsToBeDeleted队列场景2：Alter Replica时，如果”future“已经就绪，那么就可以将”current“删除了
   * 加入logsToBeDeleted队列场景3：接收到topic-delete请求时，删除TopicPartition-x
   * @param log: 名字带-delete后缀的UnifiedLog
   */
  private def addLogToBeDeleted(log: UnifiedLog): Unit = {
    // 将UnifiedLog加入logsToBeDeleted中，由schedule thread | kafka-delete-logs来删除
    // (要删除的log，加入到队列的时间点)
    this.logsToBeDeleted.add((log, time.milliseconds()))
  }

  def addStrayLog(strayPartition: TopicPartition, strayLog: UnifiedLog): Unit = {
    this.strayLogs.put(strayPartition, strayLog)
  }

  // Only for testing
  private[log] def hasLogsToBeDeleted: Boolean = !logsToBeDeleted.isEmpty

  /**
   * 目标是啥？
   * 1. 把磁盘上一个个的静态TopicPartition目录加载成LogManager可操作的UnifiedLog对象
   * 2. -delete后缀处理：删除
   * 3. --stray 流浪的partition处理？
   * 4. --future后缀处理
   */
  private[log] def loadLog(logDir: File,
                           hadCleanShutdown: Boolean,
                           recoveryPoints: Map[TopicPartition, Long],
                           logStartOffsets: Map[TopicPartition, Long],
                           defaultConfig: LogConfig,
                           topicConfigOverrides: Map[String, LogConfig],
                           numRemainingSegments: ConcurrentMap[String, Int]): UnifiedLog = {
    val topicPartition = UnifiedLog.parseTopicPartitionName(logDir)
    val config = topicConfigOverrides.getOrElse(topicPartition.topic, defaultConfig)

    // 从 recoveryPoints 和 logStartOffsets中取出 logRecoveryPoint 和 logStartOffset
    val logRecoveryPoint = recoveryPoints.getOrElse(topicPartition, 0L)
    val logStartOffset = logStartOffsets.getOrElse(topicPartition, 0L)

    // part1：初始化出UnifiedLog
    // UnifiedLog掌管某个Topic-PartitionX下所有
    val log = UnifiedLog(
      dir = logDir,
      config = config,
      logStartOffset = logStartOffset,
      recoveryPoint = logRecoveryPoint,
      maxTransactionTimeoutMs = maxTransactionTimeoutMs,
      producerStateManagerConfig = producerStateManagerConfig,
      producerIdExpirationCheckIntervalMs = producerIdExpirationCheckIntervalMs,
      scheduler = scheduler,
      time = time,
      brokerTopicStats = brokerTopicStats,
      logDirFailureChannel = logDirFailureChannel,
      lastShutdownClean = hadCleanShutdown,
      topicId = None,
      keepPartitionMetadataFile = keepPartitionMetadataFile,
      numRemainingSegments = numRemainingSegments,
      remoteStorageSystemEnable = remoteStorageSystemEnable)

    // part2：--后缀处理
    // -delete 待删除TopicPartition-x目录处理
    if (logDir.getName.endsWith(UnifiedLog.DeleteDirSuffix)) {
      // 加入logsToBeDeleted队列场景1：LogManager启动时loadLog时发现存在.delete后缀的TopicPartition-x
      addLogToBeDeleted(log)
    } else if (logDir.getName.endsWith(UnifiedLog.StrayDirSuffix)) {
      // --stray 流浪的partition？
      addStrayLog(topicPartition, log)
      warn(s"Loaded stray log: $logDir")
    } else {
      // --future
      val previous = {
        if (log.isFuture)
          this.futureLogs.put(topicPartition, log)
        else
          this.currentLogs.put(topicPartition, log)
      }
      if (previous != null) {
        if (log.isFuture)
          throw new IllegalStateException(s"Duplicate log directories found: ${log.dir.getAbsolutePath}, ${previous.dir.getAbsolutePath}")
        else
          throw new IllegalStateException(s"Duplicate log directories for $topicPartition are found in both ${log.dir.getAbsolutePath} " +
            s"and ${previous.dir.getAbsolutePath}. It is likely because log directory failure happened while broker was " +
            s"replacing current replica with future replica. Recover broker from this failure by manually deleting one of the two directories " +
            s"for this partition. It is recommended to delete the partition in the log directory that is known to have failed recently.")
      }
    }
    // 返回
    log
  }

  // factory class for naming the log recovery threads used in metrics
  class LogRecoveryThreadFactory(val dirPath: String) extends ThreadFactory {
    val threadNum = new AtomicInteger(0)

    override def newThread(runnable: Runnable): Thread = {
      KafkaThread.nonDaemon(logRecoveryThreadName(dirPath, threadNum.getAndIncrement()), runnable)
    }
  }

  // create a unique log recovery thread name for each log dir as the format: prefix-dirPath-threadNum, ex: "log-recovery-/tmp/kafkaLogs-0"
  private def logRecoveryThreadName(dirPath: String, threadNum: Int, prefix: String = "log-recovery"): String = s"$prefix-$dirPath-$threadNum"

  /*
   * decrement the number of remaining logs
   * @return the number of remaining logs after decremented 1
   */
  private[log] def decNumRemainingLogs(numRemainingLogs: ConcurrentMap[String, Int], path: String): Int = {
    require(path != null, "path cannot be null to update remaining logs metric.")
    numRemainingLogs.compute(path, (_, oldVal) => oldVal - 1)
  }

  /**
   * Recover and load all logs in the given data directories
   * 目标是什么？
   */
  private[log] def loadLogs(defaultConfig: LogConfig, topicConfigOverrides: Map[String, LogConfig]): Unit = {
    info(s"Loading logs from log dirs $liveLogDirs")
    // part1: 定义了一堆变量
    val startMs = time.hiResClockMs()
    val threadPools = ArrayBuffer.empty[ExecutorService]
    // loadLogs定义的offlineDirs
    val offlineDirs = mutable.Set.empty[(String, IOException)]
    val jobs = ArrayBuffer.empty[Seq[Future[_]]]
    var numTotalLogs = 0
    // log dir path -> number of Remaining logs map for remainingLogsToRecover metric
    val numRemainingLogs: ConcurrentMap[String, Int] = new ConcurrentHashMap[String, Int]
    // log recovery thread name -> number of remaining segments map for remainingSegmentsToRecover metric
    val numRemainingSegments: ConcurrentMap[String, Int] = new ConcurrentHashMap[String, Int]

    def handleIOException(logDirAbsolutePath: String, e: IOException): Unit = {
      offlineDirs.add((logDirAbsolutePath, e))
      error(s"Error while loading log dir $logDirAbsolutePath", e)
    }

    val uncleanLogDirs = mutable.Buffer.empty[String]
    // part2:
    // liveLogDirs -> Seq[File] | Seq[File], /datax 存储目录下的所有文件 元数据(checkpoint系列)+文件夹(Topic-Partition-x)
    for (dir <- liveLogDirs) {
      // get Path
      val logDirAbsolutePath = dir.getAbsolutePath
      // check Clean Shutdown
      var hadCleanShutdown: Boolean = false
      try {
        // 2.1 构建为了完成“loadlog”工作的线程池
        // threadPools: numRecoveryThreadsPerDataDir = num.recovery.threads.per.data.dir 默认4
        val pool = Executors.newFixedThreadPool(numRecoveryThreadsPerDataDir,
          new LogRecoveryThreadFactory(logDirAbsolutePath))
        threadPools.append(pool)

        // 2.2 check .kafka_cleanshutdown
        // LogLoader的recoverLog()方法中将会对是否cleanshutdown进行判断，来决定是否需要执行recover segment的操作
        val cleanShutdownFile = new File(dir, LogLoader.CleanShutdownFile)
        if (cleanShutdownFile.exists) {
          // Cache the clean shutdown status and use that for rest of log loading workflow. Delete the CleanShutdownFile
          // so that if broker crashes while loading the log, it is considered hard shutdown during the next boot up. KAFKA-10471
          Files.deleteIfExists(cleanShutdownFile.toPath)
          hadCleanShutdown = true
        }
        hadCleanShutdownFlags.put(logDirAbsolutePath, hadCleanShutdown)

        // 2.3 读取recovery-point-offset-checkpoint，将其信息转换成内存中的Map
        var recoveryPoints = Map[TopicPartition, Long]()
        try {
          recoveryPoints = this.recoveryPointCheckpoints(dir).read()
        } catch {
          case e: Exception =>
            warn(s"Error occurred while reading recovery-point-offset-checkpoint file of directory " +
              s"$logDirAbsolutePath, resetting the recovery checkpoint to 0", e)
        }

        // 2.4 读取log-start-offset-checkpoint，将其信息转换成内存中的Map
        var logStartOffsets = Map[TopicPartition, Long]()
        try {
          logStartOffsets = this.logStartOffsetCheckpoints(dir).read()
        } catch {
          case e: Exception =>
            warn(s"Error occurred while reading log-start-offset-checkpoint file of directory " +
              s"$logDirAbsolutePath, resetting to the base offset of the first segment", e)
        }

        // 2.5 确定哪些TopicPartition-x目录需要进行load
        // get num of  logs to Load and record it
        val logsToLoad = Option(dir.listFiles).getOrElse(Array.empty).filter(logDir =>
          logDir.isDirectory &&
            // Ignore remote-log-index-cache directory as that is index cache maintained by tiered storage subsystem
            // but not any topic-partition dir.
            !logDir.getName.equals(RemoteIndexCache.DIR_NAME) && // 排除remote-log-index-cache目录
            UnifiedLog.parseTopicPartitionName(logDir).topic != KafkaRaftServer.MetadataTopic) // 排除__cluster_metadata目录
        numTotalLogs += logsToLoad.length
        numRemainingLogs.put(logDirAbsolutePath, logsToLoad.length)
        loadLogsCompletedFlags.put(logDirAbsolutePath, logsToLoad.isEmpty)

        if (logsToLoad.isEmpty) {
          info(s"No logs found to be loaded in $logDirAbsolutePath")
        } else if (hadCleanShutdown) {
          info(s"Skipping recovery of ${logsToLoad.length} logs from $logDirAbsolutePath since " +
            "clean shutdown file was found")
        } else {
          info(s"Recovering ${logsToLoad.length} logs from $logDirAbsolutePath since no " +
            "clean shutdown file was found")
          uncleanLogDirs.append(logDirAbsolutePath)
        }

        // 2.6 使用线程池来执行 loadLog 任务
        val jobsForDir = logsToLoad.map { logDir =>
          // a. 定义任务 define runnable
          val runnable: Runnable = () => {
            debug(s"Loading log $logDir")
            var log = None: Option[UnifiedLog]
            val logLoadStartMs = time.hiResClockMs()
            try {
              // **loadLog**
              // 注意LogManager中定义的2个变量currentLogs和futureLogs定义的map结构
              // loadLog之后会把load出来的UnifiedLog纳入管理其中
              log = Some(loadLog(logDir, hadCleanShutdown, recoveryPoints, logStartOffsets,
                defaultConfig, topicConfigOverrides, numRemainingSegments))
            } catch {
              // load过程中发生IOException，调用handleIOException
              case e: IOException =>
                handleIOException(logDirAbsolutePath, e)
              case e: KafkaStorageException if e.getCause.isInstanceOf[IOException] =>
                // KafkaStorageException might be thrown, ex: during writing LeaderEpochFileCache
                // And while converting IOException to KafkaStorageException, we've already handled the exception. So we can ignore it here.
            } finally {
              val logLoadDurationMs = time.hiResClockMs() - logLoadStartMs
              val remainingLogs = decNumRemainingLogs(numRemainingLogs, logDirAbsolutePath)
              val currentNumLoaded = logsToLoad.length - remainingLogs
              log match {
                case Some(loadedLog) => info(s"Completed load of $loadedLog with ${loadedLog.numberOfSegments} segments, " +
                  s"local-log-start-offset ${loadedLog.localLogStartOffset()} and log-end-offset ${loadedLog.logEndOffset} in ${logLoadDurationMs}ms " +
                  s"($currentNumLoaded/${logsToLoad.length} completed in $logDirAbsolutePath)")
                case None => info(s"Error while loading logs in $logDir in ${logLoadDurationMs}ms ($currentNumLoaded/${logsToLoad.length} completed in $logDirAbsolutePath)")
              }

              if (remainingLogs == 0) {
                // loadLog is completed for all logs under the logDdir, mark it.
                loadLogsCompletedFlags.put(logDirAbsolutePath, true)
              }
            }
          }
          runnable
        }
        // b. 提交任务 submit job
        jobs += jobsForDir.map(pool.submit)
      } catch {
        case e: IOException =>
          handleIOException(logDirAbsolutePath, e)
      }
    }

    // part3: 确保等待任务完成 + 线程关闭
    try {
      addLogRecoveryMetrics(numRemainingLogs, numRemainingSegments)
      // future.get() 确保每个loadlog任务都执行完成
      for (dirJobs <- jobs) {
        dirJobs.foreach(_.get)
      }
      // load结束，遍历offlineDirs，加入offlineLogDirQueue
      offlineDirs.foreach { case (dir, e) =>
        logDirFailureChannel.maybeAddOfflineLogDir(dir, s"Error while loading log dir $dir", e)
      }
    } catch {
      case e: ExecutionException =>
        error(s"There was an error in one of the threads during logs loading: ${e.getCause}")
        throw e.getCause
    } finally {
      removeLogRecoveryMetrics()
      // 关闭每个线程
      threadPools.foreach(_.shutdown())
    }

    val elapsedMs = time.hiResClockMs() - startMs
    val printedUncleanLogDirs = if (uncleanLogDirs.isEmpty) "" else s" (unclean log dirs = $uncleanLogDirs)"
    info(s"Loaded $numTotalLogs logs in ${elapsedMs}ms$printedUncleanLogDirs")
  }

  private[log] def addLogRecoveryMetrics(numRemainingLogs: ConcurrentMap[String, Int],
                                         numRemainingSegments: ConcurrentMap[String, Int]): Unit = {
    debug("Adding log recovery metrics")
    for (dir <- logDirs) {
      metricsGroup.newGauge("remainingLogsToRecover", () => numRemainingLogs.get(dir.getAbsolutePath),
        Map("dir" -> dir.getAbsolutePath).asJava)
      for (i <- 0 until numRecoveryThreadsPerDataDir) {
        val threadName = logRecoveryThreadName(dir.getAbsolutePath, i)
        metricsGroup.newGauge("remainingSegmentsToRecover", () => numRemainingSegments.get(threadName),
          Map("dir" -> dir.getAbsolutePath, "threadNum" -> i.toString).asJava)
      }
    }
  }

  private[log] def removeLogRecoveryMetrics(): Unit = {
    debug("Removing log recovery metrics")
    for (dir <- logDirs) {
      metricsGroup.removeMetric("remainingLogsToRecover", Map("dir" -> dir.getAbsolutePath).asJava)
      for (i <- 0 until numRecoveryThreadsPerDataDir) {
        metricsGroup.removeMetric("remainingSegmentsToRecover", Map("dir" -> dir.getAbsolutePath, "threadNum" -> i.toString).asJava)
      }
    }
  }

  /**
   *  Start the background threads to flush logs and do log cleanup
   */
  def startup(topicNames: Set[String]): Unit = {
    // ensure consistency between default config and overrides
    val defaultConfig = currentDefaultConfig
    startupWithConfigOverrides(defaultConfig, fetchTopicConfigOverrides(defaultConfig, topicNames))
  }

  // visible for testing
  @nowarn("cat=deprecation")
  private[log] def fetchTopicConfigOverrides(defaultConfig: LogConfig, topicNames: Set[String]): Map[String, LogConfig] = {
    val topicConfigOverrides = mutable.Map[String, LogConfig]()
    val defaultProps = defaultConfig.originals()
    topicNames.foreach { topicName =>
      var overrides = configRepository.topicConfig(topicName)
      // save memory by only including configs for topics with overrides
      if (!overrides.isEmpty) {
        Option(overrides.getProperty(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG)).foreach { versionString =>
          val messageFormatVersion = new MessageFormatVersion(versionString, interBrokerProtocolVersion.version)
          if (messageFormatVersion.shouldIgnore) {
            val copy = new Properties()
            copy.putAll(overrides)
            copy.remove(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG)
            overrides = copy

            if (messageFormatVersion.shouldWarn)
              warn(messageFormatVersion.topicWarningMessage(topicName))
          }
        }

        val logConfig = LogConfig.fromProps(defaultProps, overrides)
        topicConfigOverrides(topicName) = logConfig
      }
    }
    topicConfigOverrides
  }

  private def fetchLogConfig(topicName: String): LogConfig = {
    // ensure consistency between default config and overrides
    val defaultConfig = currentDefaultConfig
    fetchTopicConfigOverrides(defaultConfig, Set(topicName)).values.headOption.getOrElse(defaultConfig)
  }

  // visible for testing
  // broker启动时，LogManager完成的事情, 3件事情
  // 1. loadlog
  // 2. 5个schedule任务
  // 3. logCleaner线程启动
  private[log] def startupWithConfigOverrides(defaultConfig: LogConfig, topicConfigOverrides: Map[String, LogConfig]): Unit = {
    // load Logs when broker starting ...
    // *this could take a while if shutdown was not clean
    loadLogs(defaultConfig, topicConfigOverrides)

    /* Schedule the cleanup task to delete old logs */
    if (scheduler != null) {
      info("Starting log cleanup with a period of %d ms.".format(retentionCheckMs))
      scheduler.schedule("kafka-log-retention",
                         () => cleanupLogs(),
                         InitialTaskDelayMs,
                         retentionCheckMs) // 5min
      info("Starting log flusher with a default period of %d ms.".format(flushCheckMs))
      scheduler.schedule("kafka-log-flusher",
                         () => flushDirtyLogs(),
                         InitialTaskDelayMs,
                         flushCheckMs) // 1000L
      scheduler.schedule("kafka-recovery-point-checkpoint",
                         () => checkpointLogRecoveryOffsets(),
                         InitialTaskDelayMs,
                         flushRecoveryOffsetCheckpointMs) // 10000L
      scheduler.schedule("kafka-log-start-offset-checkpoint",
                         () => checkpointLogStartOffsets(),
                         InitialTaskDelayMs,
                         flushStartOffsetCheckpointMs) // 10000L
      scheduler.scheduleOnce("kafka-delete-logs", // will be rescheduled after each delete logs with a dynamic period
                         () => deleteLogs(),
                         InitialTaskDelayMs) // 30S
    }

    // LogCleaner线程入口
    if (cleanerConfig.enableCleaner) {
      _cleaner = new LogCleaner(cleanerConfig, liveLogDirs, currentLogs, logDirFailureChannel, time = time)
      _cleaner.startup()
    }
  }

  /**
   * Close all the logs
   * kafka正常关闭时，LogManager所需做的关闭动作
   */
  def shutdown(): Unit = {
    info("Shutting down.")

    metricsGroup.removeMetric("OfflineLogDirectoryCount")
    for (dir <- logDirs) {
      metricsGroup.removeMetric("LogDirectoryOffline", Map("logDirectory" -> dir.getAbsolutePath).asJava)
    }

    val threadPools = ArrayBuffer.empty[ExecutorService]
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]

    // stop the cleaner first
    if (cleaner != null) {
      CoreUtils.swallow(cleaner.shutdown(), this)
    }

    val localLogsByDir = logsByDir

    // close logs in each dir
    for (dir <- liveLogDirs) {
      debug(s"Flushing and closing logs at $dir")

      val pool = Executors.newFixedThreadPool(numRecoveryThreadsPerDataDir,
        KafkaThread.nonDaemon(s"log-closing-${dir.getAbsolutePath}", _))
      threadPools.append(pool)

      val logs = logsInDir(localLogsByDir, dir).values

      val jobsForDir = logs.map { log =>
        val runnable: Runnable = () => {
          // kafka正常关闭情况下，会整体flush一把，其中更新了recovery point值
          // flush the log to ensure latest possible recovery point
          log.flush(true)
          log.close()
        }
        runnable
      }

      jobs(dir) = jobsForDir.map(pool.submit).toSeq
    }

    try {
      jobs.forKeyValue { (dir, dirJobs) =>
        if (waitForAllToComplete(dirJobs,
          e => warn(s"There was an error in one of the threads during LogManager shutdown: ${e.getCause}"))) {
          val logs = logsInDir(localLogsByDir, dir)

          // update the last flush point
          // 更新recovery point至磁盘上
          debug(s"Updating recovery points at $dir")
          checkpointRecoveryOffsetsInDir(dir, logs)

          debug(s"Updating log start offsets at $dir")
          checkpointLogStartOffsetsInDir(dir, logs)

          // mark that the shutdown was clean by creating marker file for log dirs that:
          //  1. had clean shutdown marker file; or
          //  2. had no clean shutdown marker file, but all logs under it have been recovered at startup time
          val logDirAbsolutePath = dir.getAbsolutePath
          if (hadCleanShutdownFlags.getOrDefault(logDirAbsolutePath, false) ||
              loadLogsCompletedFlags.getOrDefault(logDirAbsolutePath, false)) {
            debug(s"Writing clean shutdown marker at $dir")
            CoreUtils.swallow(Files.createFile(new File(dir, LogLoader.CleanShutdownFile).toPath), this)
          }
        }
      }
    } finally {
      threadPools.foreach(_.shutdown())
      // regardless of whether the close succeeded, we need to unlock the data directories
      dirLocks.foreach(_.destroy())
    }

    info("Shutdown complete.")
  }

  /**
   * Truncate the partition logs to the specified offsets and checkpoint the recovery point to this offset
   *
   * @param partitionOffsets Partition logs that need to be truncated
   * @param isFuture True iff the truncation should be performed on the future log of the specified partitions
   */
  def truncateTo(partitionOffsets: Map[TopicPartition, Long], isFuture: Boolean): Unit = {
    val affectedLogs = ArrayBuffer.empty[UnifiedLog]

    // 1. 遍历Map
    for ((topicPartition, truncateOffset) <- partitionOffsets) {
      // 确定要操作的UnifiedLog
      val log = {
        if (isFuture)
          futureLogs.get(topicPartition)
        else
          currentLogs.get(topicPartition)
      }
      // If the log does not exist, skip it
      if (log != null) {
        // May need to abort and pause the cleaning of the log, and resume after truncation is done.
        // logCleaner是不会清理activeSegment的，但是你想截取的【truncateOffset < log.activeSegment.baseOffset】
        // 意味着 你想截取的部分，可能处于logCleaner正在Clean的地方，为了避免2者冲突，需要暂停此UnifiedLog的Clean动作
        val needToStopCleaner = truncateOffset < log.activeSegment.baseOffset
        if (needToStopCleaner && !isFuture)
          abortAndPauseCleaning(topicPartition)

        try {
          // 执行truncate动作
          if (log.truncateTo(truncateOffset))
            affectedLogs += log

          // 可能需要更新CleanerCheckpoint
          if (needToStopCleaner && !isFuture)
            maybeTruncateCleanerCheckpointToActiveSegmentBaseOffset(log, topicPartition)
        } finally {
          // 恢复Cleaner
          if (needToStopCleaner && !isFuture)
            resumeCleaning(topicPartition)
        }
      }
    }
    // 为啥要刷一把recovery checkpoint？
    // 因为truncate会updateLogEndOffset(targetOffset)
    // 调用updateLogEndOffset来更新LEO，其中会判断更新recovery checkpoint，来保证recovery checkpoint 低于 LEO
    // 所以刷了一把recovery checkpoint
    for (dir <- affectedLogs.map(_.parentDirFile).distinct) {
      checkpointRecoveryOffsetsInDir(dir)
    }
  }

  /**
   * Delete all data in a partition and start the log at the new offset
   *
   * @param topicPartition The partition whose log needs to be truncated
   * @param newOffset The new offset to start the log with
   * @param isFuture True iff the truncation should be performed on the future log of the specified partition
   * @param logStartOffsetOpt The log start offset to set for the log. If None, the new offset will be used.
   */
  def truncateFullyAndStartAt(topicPartition: TopicPartition,
                              newOffset: Long,
                              isFuture: Boolean,
                              logStartOffsetOpt: Option[Long] = None): Unit = {
    val log = {
      if (isFuture)
        futureLogs.get(topicPartition)
      else
        currentLogs.get(topicPartition)
    }
    // If the log does not exist, skip it
    if (log != null) {
      // Abort and pause the cleaning of the log, and resume after truncation is done.
      if (!isFuture)
        abortAndPauseCleaning(topicPartition)
      try {
        log.truncateFullyAndStartAt(newOffset, logStartOffsetOpt)
        if (!isFuture)
          maybeTruncateCleanerCheckpointToActiveSegmentBaseOffset(log, topicPartition)
      } finally {
        if (!isFuture)
          resumeCleaning(topicPartition)
      }
      checkpointRecoveryOffsetsInDir(log.parentDirFile)
    }
  }

  /**
   * Write out the current recovery point for all logs to a text file in the log directory
   * to avoid recovering the whole log on startup.
   */
  def checkpointLogRecoveryOffsets(): Unit = {
    val logsByDirCached = logsByDir
    liveLogDirs.foreach { logDir =>
      val logsToCheckpoint = logsInDir(logsByDirCached, logDir)
      checkpointRecoveryOffsetsInDir(logDir, logsToCheckpoint)
    }
  }

  /**
   * Write out the current log start offset for all logs to a text file in the log directory
   * to avoid exposing data that have been deleted by DeleteRecordsRequest
   */
  def checkpointLogStartOffsets(): Unit = {
    val logsByDirCached = logsByDir
    liveLogDirs.foreach { logDir =>
      checkpointLogStartOffsetsInDir(logDir, logsInDir(logsByDirCached, logDir))
    }
  }

  /**
   * Checkpoint recovery offsets for all the logs in logDir.
   *
   * @param logDir the directory in which the logs to be checkpointed are
   */
  // Only for testing
  private[log] def checkpointRecoveryOffsetsInDir(logDir: File): Unit = {
    checkpointRecoveryOffsetsInDir(logDir, logsInDir(logDir))
  }

  /**
   * Checkpoint recovery offsets for all the provided logs.
   *
   * @param logDir the directory in which the logs are
   * @param logsToCheckpoint the logs to be checkpointed
   */
  private def checkpointRecoveryOffsetsInDir(logDir: File, logsToCheckpoint: Map[TopicPartition, UnifiedLog]): Unit = {
    try {
      // 每个UnifiedLog都维护了recoveryPoint，所以要看recoveryPoint的更新
      recoveryPointCheckpoints.get(logDir).foreach { checkpoint =>
        val recoveryOffsets = logsToCheckpoint.map { case (tp, log) => tp -> log.recoveryPoint }
        // checkpoint.write calls Utils.atomicMoveWithFallback, which flushes the parent
        // directory and guarantees crash consistency.
        checkpoint.write(recoveryOffsets)
      }
    } catch {
      case e: KafkaStorageException =>
        error(s"Disk error while writing recovery offsets checkpoint in directory $logDir: ${e.getMessage}")
      case e: IOException =>
        logDirFailureChannel.maybeAddOfflineLogDir(logDir.getAbsolutePath,
          s"Disk error while writing recovery offsets checkpoint in directory $logDir: ${e.getMessage}", e)
    }
  }

  /**
   * Checkpoint log start offsets for all the provided logs in the provided directory.
   *
   * @param logDir the directory in which logs are checkpointed
   * @param logsToCheckpoint the logs to be checkpointed
   */
  private def checkpointLogStartOffsetsInDir(logDir: File, logsToCheckpoint: Map[TopicPartition, UnifiedLog]): Unit = {
    try {
      logStartOffsetCheckpoints.get(logDir).foreach { checkpoint =>
        val logStartOffsets = logsToCheckpoint.collect {
          case (tp, log) if log.logStartOffset > log.logSegments.head.baseOffset => tp -> log.logStartOffset
        }
        checkpoint.write(logStartOffsets)
      }
    } catch {
      case e: KafkaStorageException =>
        error(s"Disk error while writing log start offsets checkpoint in directory $logDir: ${e.getMessage}")
    }
  }

  // The logDir should be an absolute path
  def maybeUpdatePreferredLogDir(topicPartition: TopicPartition, logDir: String): Unit = {
    // Do not cache the preferred log directory if either the current log or the future log for this partition exists in the specified logDir
    if (!getLog(topicPartition).exists(_.parentDir == logDir) &&
        !getLog(topicPartition, isFuture = true).exists(_.parentDir == logDir))
      preferredLogDirs.put(topicPartition, logDir)
  }

  /**
   * Abort and pause cleaning of the provided partition and log a message about it.
   */
  def abortAndPauseCleaning(topicPartition: TopicPartition): Unit = {
    if (cleaner != null) {
      cleaner.abortAndPauseCleaning(topicPartition)
      info(s"The cleaning for partition $topicPartition is aborted and paused")
    }
  }

  /**
   * Abort cleaning of the provided partition and log a message about it.
   */
  def abortCleaning(topicPartition: TopicPartition): Unit = {
    if (cleaner != null) {
      cleaner.abortCleaning(topicPartition)
      info(s"The cleaning for partition $topicPartition is aborted")
    }
  }

  /**
   * Resume cleaning of the provided partition and log a message about it.
   */
  private def resumeCleaning(topicPartition: TopicPartition): Unit = {
    if (cleaner != null) {
      cleaner.resumeCleaning(Seq(topicPartition))
      info(s"Cleaning for partition $topicPartition is resumed")
    }
  }

  /**
   * Truncate the cleaner's checkpoint to the based offset of the active segment of
   * the provided log.
   */
  private def maybeTruncateCleanerCheckpointToActiveSegmentBaseOffset(log: UnifiedLog, topicPartition: TopicPartition): Unit = {
    if (cleaner != null) {
      cleaner.maybeTruncateCheckpoint(log.parentDirFile, topicPartition, log.activeSegment.baseOffset)
    }
  }

  /**
   * Get the log if it exists, otherwise return None
   *
   * @param topicPartition the partition of the log
   * @param isFuture True iff the future log of the specified partition should be returned
   */
  def getLog(topicPartition: TopicPartition, isFuture: Boolean = false): Option[UnifiedLog] = {
    if (isFuture)
      Option(futureLogs.get(topicPartition))
    else
      Option(currentLogs.get(topicPartition))
  }

  /**
   * Method to indicate that logs are getting initialized for the partition passed in as argument.
   * This method should always be followed by [[kafka.log.LogManager#finishedInitializingLog]] to indicate that log
   * initialization is done.
   */
  def initializingLog(topicPartition: TopicPartition): Unit = {
    partitionsInitializing(topicPartition) = false
  }

  /**
   * Mark the partition configuration for all partitions that are getting initialized for topic
   * as dirty. That will result in reloading of configuration once initialization is done.
   */
  def topicConfigUpdated(topic: String): Unit = {
    partitionsInitializing.keys.filter(_.topic() == topic).foreach {
      topicPartition => partitionsInitializing.replace(topicPartition, false, true)
    }
  }

  /**
   * Update the configuration of the provided topic.
   */
  def updateTopicConfig(topic: String,
                        newTopicConfig: Properties,
                        isRemoteLogStorageSystemEnabled: Boolean): Unit = {
    topicConfigUpdated(topic)
    val logs = logsByTopic(topic)
    // Combine the default properties with the overrides in zk to create the new LogConfig
    val newLogConfig = LogConfig.fromProps(currentDefaultConfig.originals, newTopicConfig)
    // We would like to validate the configuration no matter whether the logs have materialised on disk or not.
    // Otherwise we risk someone creating a tiered-topic, disabling Tiered Storage cluster-wide and the check
    // failing since the logs for the topic are non-existent.
    LogConfig.validateRemoteStorageOnlyIfSystemEnabled(newLogConfig.values(), isRemoteLogStorageSystemEnabled, true)
    if (logs.nonEmpty) {
      logs.foreach { log =>
        val oldLogConfig = log.updateConfig(newLogConfig)
        if (oldLogConfig.compact && !newLogConfig.compact) {
          abortCleaning(log.topicPartition)
        }
      }
    }
  }

  /**
   * Mark all in progress partitions having dirty configuration if broker configuration is updated.
   */
  def brokerConfigUpdated(): Unit = {
    partitionsInitializing.keys.foreach {
      topicPartition => partitionsInitializing.replace(topicPartition, false, true)
    }
  }

  /**
   * Method to indicate that the log initialization for the partition passed in as argument is
   * finished. This method should follow a call to [[kafka.log.LogManager#initializingLog]].
   *
   * It will retrieve the topic configs a second time if they were updated while the
   * relevant log was being loaded.
   */
  def finishedInitializingLog(topicPartition: TopicPartition,
                              maybeLog: Option[UnifiedLog]): Unit = {
    val removedValue = partitionsInitializing.remove(topicPartition)
    if (removedValue.contains(true))
      maybeLog.foreach(_.updateConfig(fetchLogConfig(topicPartition.topic)))
  }

  /**
   * If the log already exists, just return a copy of the existing log
   * Otherwise if isNew=true or if there is no offline log directory, create a log for the given topic and the given partition
   * Otherwise throw KafkaStorageException
   *
   * @param topicPartition The partition whose log needs to be returned or created
   * @param isNew Whether the replica should have existed on the broker or not
   * @param isFuture True if the future log of the specified partition should be returned or created
   * @param topicId The topic ID of the partition's topic
   * @throws KafkaStorageException if isNew=false, log is not found in the cache and there is offline log directory on the broker
   * @throws InconsistentTopicIdException if the topic ID in the log does not match the topic ID provided
   */
  def getOrCreateLog(topicPartition: TopicPartition, isNew: Boolean = false, isFuture: Boolean = false, topicId: Option[Uuid]): UnifiedLog = {
    logCreationOrDeletionLock synchronized {
      val log = getLog(topicPartition, isFuture).getOrElse {
        // 如果一个dir是offline的，无法在offlineLogDirs创建新的topic
        // create the log if it has not already been created in another thread
        if (!isNew && offlineLogDirs.nonEmpty)
          throw new KafkaStorageException(s"Can not create log for $topicPartition because log directories ${offlineLogDirs.mkString(",")} are offline")

        val logDirs: List[File] = {
          val preferredLogDir = preferredLogDirs.get(topicPartition)

          if (isFuture) {
            if (preferredLogDir == null)
              throw new IllegalStateException(s"Can not create the future log for $topicPartition without having a preferred log directory")
            else if (getLog(topicPartition).get.parentDir == preferredLogDir)
              throw new IllegalStateException(s"Can not create the future log for $topicPartition in the current log directory of this partition")
          }

          if (preferredLogDir != null)
            List(new File(preferredLogDir))
          else
            nextLogDirs()
        }

        val logDirName = {
          if (isFuture)
            UnifiedLog.logFutureDirName(topicPartition)
          else
            UnifiedLog.logDirName(topicPartition)
        }

        val logDir = logDirs
          .iterator // to prevent actually mapping the whole list, lazy map
          .map(createLogDirectory(_, logDirName))
          .find(_.isSuccess)
          .getOrElse(Failure(new KafkaStorageException("No log directories available. Tried " + logDirs.map(_.getAbsolutePath).mkString(", "))))
          .get // If Failure, will throw

        val config = fetchLogConfig(topicPartition.topic)
        // will call [UnifiedLog] -> object.apply(..)
        val log = UnifiedLog(
          dir = logDir,
          config = config,
          logStartOffset = 0L,
          recoveryPoint = 0L,
          maxTransactionTimeoutMs = maxTransactionTimeoutMs,
          producerStateManagerConfig = producerStateManagerConfig,
          producerIdExpirationCheckIntervalMs = producerIdExpirationCheckIntervalMs,
          scheduler = scheduler,
          time = time,
          brokerTopicStats = brokerTopicStats,
          logDirFailureChannel = logDirFailureChannel,
          topicId = topicId,
          keepPartitionMetadataFile = keepPartitionMetadataFile,
          remoteStorageSystemEnable = remoteStorageSystemEnable)

        if (isFuture)
          futureLogs.put(topicPartition, log)
        else
          currentLogs.put(topicPartition, log)

        info(s"Created log for partition $topicPartition in $logDir with properties ${config.overriddenConfigsAsLoggableString}")
        // Remove the preferred log dir since it has already been satisfied
        preferredLogDirs.remove(topicPartition)

        log
      }
      // When running a ZK controller, we may get a log that does not have a topic ID. Assign it here.
      if (log.topicId.isEmpty) {
        topicId.foreach(log.assignTopicId)
      }

      // Ensure topic IDs are consistent
      topicId.foreach { topicId =>
        log.topicId.foreach { logTopicId =>
          if (topicId != logTopicId)
            throw new InconsistentTopicIdException(s"Tried to assign topic ID $topicId to log for topic partition $topicPartition," +
              s"but log already contained topic ID $logTopicId")
        }
      }
      log
    }
  }

  private[log] def createLogDirectory(logDir: File, logDirName: String): Try[File] = {
    val logDirPath = logDir.getAbsolutePath
    if (isLogDirOnline(logDirPath)) {
      val dir = new File(logDirPath, logDirName)
      try {
        Files.createDirectories(dir.toPath)
        Success(dir)
      } catch {
        case e: IOException =>
          // LogManager创建TopicPartition本地目录时，发生IOException
          val msg = s"Error while creating log for $logDirName in dir $logDirPath"
          logDirFailureChannel.maybeAddOfflineLogDir(logDirPath, msg, e)
          warn(msg, e)
          Failure(new KafkaStorageException(msg, e))
      }
    } else {
      Failure(new KafkaStorageException(s"Can not create log $logDirName because log directory $logDirPath is offline"))
    }
  }

  /**
   *  thread kafka-delete-logs take from "logsToBeDeleted" to delete asynly
   *  Delete logs marked for deletion. Delete all logs for which `currentDefaultConfig.fileDeleteDelayMs`
   *  has elapsed after the delete was scheduled. Logs for which this interval has not yet elapsed will be
   *  considered for deletion in the next iteration of `deleteLogs`. The next iteration will be executed
   *  after the remaining time for the first log that is not deleted. If there are no more `logsToBeDeleted`,
   *  `deleteLogs` will be executed after `currentDefaultConfig.fileDeleteDelayMs`.
   */
  private def deleteLogs(): Unit = {
    var nextDelayMs = 0L
    // "The time to wait before deleting a file from the " + "filesystem", 默认60S
    val fileDeleteDelayMs = currentDefaultConfig.fileDeleteDelayMs
    try {
      // 计算下次执行删除延迟多久
      def nextDeleteDelayMs: Long = {
        if (!logsToBeDeleted.isEmpty) {
          val (_, scheduleTimeMs) = logsToBeDeleted.peek()
          scheduleTimeMs + fileDeleteDelayMs - time.milliseconds()
        } else
          fileDeleteDelayMs
      }
      // nextDelayMs <= 0 即还没有到达执行删除的时间
      while ({nextDelayMs = nextDeleteDelayMs; nextDelayMs <= 0}) {
        // 删除场景：删除是UnifiedLog级别的，logsToBeDeleted队列中不为空场景
        // 会根据其定义的“延迟”删除时间，来不断删除
        // 那么问题来了？ -> 什么场景下log会被加入logsToBeDeleted呢？
        // 取出待删除的UnifiedLog
        val (removedLog, _) = logsToBeDeleted.take()
        if (removedLog != null) {
          try {
            // 调用UnifiedLog的删除能力
            removedLog.delete()
            info(s"Deleted log for partition ${removedLog.topicPartition} in ${removedLog.dir.getAbsolutePath}.")
          } catch {
            case e: KafkaStorageException =>
              error(s"Exception while deleting $removedLog in dir ${removedLog.parentDir}.", e)
          }
        }
      }
    } catch {
      case e: Throwable =>
        error(s"Exception in kafka-delete-logs thread.", e)
    } finally {
      try {
        // 虽然是scheduleOnce的，但是递归掉的， 主要是为了计算出准确的nextDelayMs，而不是没有可删除的.delete的时候，也不停scheduler调用
        scheduler.scheduleOnce("kafka-delete-logs",
          () => deleteLogs(),
          nextDelayMs)
      } catch {
        case e: Throwable =>
          // No errors should occur unless scheduler has been shutdown
          error(s"Failed to schedule next delete in kafka-delete-logs thread", e)
      }
    }
  }

  /**
    * Mark the partition directory in the source log directory for deletion and
    * rename the future log of this partition in the destination log directory to be the current log
    *
    * @param topicPartition TopicPartition that needs to be swapped
    */
  def replaceCurrentWithFutureLog(topicPartition: TopicPartition): Unit = {
    logCreationOrDeletionLock synchronized {
      val sourceLog = currentLogs.get(topicPartition)
      val destLog = futureLogs.get(topicPartition)

      info(s"Attempting to replace current log $sourceLog with $destLog for $topicPartition")
      if (sourceLog == null)
        throw new KafkaStorageException(s"The current replica for $topicPartition is offline")
      if (destLog == null)
        throw new KafkaStorageException(s"The future replica for $topicPartition is offline")

      destLog.renameDir(UnifiedLog.logDirName(topicPartition), true)
      // the metrics tags still contain "future", so we have to remove it.
      // we will add metrics back after sourceLog remove the metrics
      destLog.removeLogMetrics()
      destLog.updateHighWatermark(sourceLog.highWatermark)

      // Now that future replica has been successfully renamed to be the current replica
      // Update the cached map and log cleaner as appropriate.
      futureLogs.remove(topicPartition)
      currentLogs.put(topicPartition, destLog)
      if (cleaner != null) {
        cleaner.alterCheckpointDir(topicPartition, sourceLog.parentDirFile, destLog.parentDirFile)
        resumeCleaning(topicPartition)
      }

      try {
        // 加入logsToBeDeleted队列场景2：Alter Replica时，如果”future“已经就绪，那么就可以给本地的”current“带上-delete后缀了
        sourceLog.renameDir(UnifiedLog.logDeleteDirName(topicPartition), true)
        // Now that replica in source log directory has been successfully renamed for deletion.
        // Close the log, update checkpoint files, and enqueue this log to be deleted.
        sourceLog.close()
        val logDir = sourceLog.parentDirFile
        val logsToCheckpoint = logsInDir(logDir)
        checkpointRecoveryOffsetsInDir(logDir, logsToCheckpoint)
        checkpointLogStartOffsetsInDir(logDir, logsToCheckpoint)
        sourceLog.removeLogMetrics()
        destLog.newMetrics()
        // 加入logsToBeDeleted队列
        addLogToBeDeleted(sourceLog)
      } catch {
        case e: KafkaStorageException =>
          // If sourceLog's log directory is offline, we need close its handlers here.
          // handleLogDirFailure() will not close handlers of sourceLog because it has been removed from currentLogs map
          sourceLog.closeHandlers()
          sourceLog.removeLogMetrics()
          throw e
      }

      info(s"The current replica is successfully replaced with the future replica for $topicPartition")
    }
  }

  /**
    * "delete-topic" request will call the "underlying storage layer" API
    * Rename the directory of the given topic-partition "logdir" as "logdir.uuid.delete" and
    * add it in the queue for deletion.
    *
    * @param topicPartition TopicPartition that needs to be deleted
    * @param isFuture True iff the future log of the specified partition should be deleted
    * @param checkpoint True if checkpoints must be written
    * @return the removed log
    */
  def asyncDelete(topicPartition: TopicPartition,
                  isFuture: Boolean = false,
                  checkpoint: Boolean = true,
                  isStray: Boolean = false): Option[UnifiedLog] = {
    // 1. Use synchronization locks to ensure thread safety
    val removedLog: Option[UnifiedLog] = logCreationOrDeletionLock synchronized {
      // remove and return the Log of "mark for remove topicPartition in futureLogs and currentLogs"
      removeLogAndMetrics(if (isFuture) futureLogs else currentLogs, topicPartition)
    }

    // 2. handle the above returned removedLog
    removedLog match {
      case Some(removedLog) =>
        // 2.1 We need to wait until there is no more cleaning task on the log to be deleted before actually deleting it.
        if (cleaner != null && !isFuture) {
          // abort Cleaning
          cleaner.abortCleaning(topicPartition)
          // update checkpoint
          if (checkpoint) {
            cleaner.updateCheckpoints(removedLog.parentDirFile, partitionToRemove = Option(topicPartition))
          }
        }

        // 2.2 rename related dir "-stray" and "-delete"
        if (isStray) {
          // Move aside stray partitions, don't delete them
          removedLog.renameDir(UnifiedLog.logStrayDirName(topicPartition), false)
          warn(s"Log for partition ${removedLog.topicPartition} is marked as stray and renamed to ${removedLog.dir.getAbsolutePath}")
        } else {
          // 加入logsToBeDeleted队列场景3：接收到topic-delete请求时，删除TopicPartition-x
          // 先改名给Topic-Partition-x带上.delete
          removedLog.renameDir(UnifiedLog.logDeleteDirName(topicPartition), false)
          // 将该TopicPartition加入logsToBeDeleted队列
          // * add removeLog to logsToBeDeleted, will be deleted asynchronously later
          addLogToBeDeleted(removedLog)
          info(s"Log for partition ${removedLog.topicPartition} is renamed to ${removedLog.dir.getAbsolutePath} and is scheduled for deletion")
        }

        // 2.3 True if checkpoints must be written
        if (checkpoint) {
          // handle checkpoints
          val logDir = removedLog.parentDirFile
          val logsToCheckpoint = logsInDir(logDir)
          checkpointRecoveryOffsetsInDir(logDir, logsToCheckpoint)
          checkpointLogStartOffsetsInDir(logDir, logsToCheckpoint)
        }

      case None =>
        if (offlineLogDirs.nonEmpty) {
          throw new KafkaStorageException(s"Failed to delete log for ${if (isFuture) "future" else ""} $topicPartition because it may be in one of the offline directories ${offlineLogDirs.mkString(",")}")
        }
    }
    // 3. return
    removedLog
  }

  /**
   * Rename the directories of the given topic-partitions and add them in the queue for
   * deletion. Checkpoints are updated once all the directories have been renamed.
   *
   * @param topicPartitions The set of topic-partitions to delete asynchronously
   * @param errorHandler The error handler that will be called when a exception for a particular
   *                     topic-partition is raised
   */
  def asyncDelete(topicPartitions: Set[TopicPartition],
                  isStray: Boolean,
                  errorHandler: (TopicPartition, Throwable) => Unit): Unit = {
    val logDirs = mutable.Set.empty[File]

    topicPartitions.foreach { topicPartition =>
      try {
        getLog(topicPartition).foreach { log =>
          logDirs += log.parentDirFile
          asyncDelete(topicPartition, checkpoint = false, isStray = isStray)
        }
        getLog(topicPartition, isFuture = true).foreach { log =>
          logDirs += log.parentDirFile
          asyncDelete(topicPartition, isFuture = true, checkpoint = false, isStray = isStray)
        }
      } catch {
        case e: Throwable => errorHandler(topicPartition, e)
      }
    }

    val logsByDirCached = logsByDir
    logDirs.foreach { logDir =>
      if (cleaner != null) cleaner.updateCheckpoints(logDir)
      val logsToCheckpoint = logsInDir(logsByDirCached, logDir)
      checkpointRecoveryOffsetsInDir(logDir, logsToCheckpoint)
      checkpointLogStartOffsetsInDir(logDir, logsToCheckpoint)
    }
  }

  /**
   * Provides the full ordered list of suggested directories for the next partition.
   * Currently this is done by calculating the number of partitions in each directory and then sorting the
   * data directories by fewest partitions.
   */
  private def nextLogDirs(): List[File] = {
    if(_liveLogDirs.size == 1) {
      List(_liveLogDirs.peek())
    } else {
      // count the number of logs in each parent directory (including 0 for empty directories
      val logCounts = allLogs.groupBy(_.parentDir).map { case (parent, logs) => parent -> logs.size }
      val zeros = _liveLogDirs.asScala.map(dir => (dir.getPath, 0)).toMap
      val dirCounts = (zeros ++ logCounts).toBuffer

      // choose the directory with the least logs in it
      dirCounts.sortBy(_._2).map {
        case (path: String, _: Int) => new File(path)
      }.toList
    }
  }

  /**
   * Delete any eligible logs. Return the number of segments deleted.
   * Only consider logs that are not compacted.
   * 针对非compact的UnifiedLog执行删除操作
   */
  def cleanupLogs(): Unit = {
    debug("Beginning log cleanup...")
    var total = 0
    val startMs = time.milliseconds

    // clean current logs.
    // 1. 排除compact的UnifiedLog
    val deletableLogs = {
      if (cleaner != null) {
        // prevent cleaner from working on same partitions when changing cleanup policy
        cleaner.pauseCleaningForNonCompactedPartitions()
      } else {
        // "Only consider logs that are not compacted."
        currentLogs.filter {
          case (_, log) => !log.config.compact
        }
      }
    }
    // 2. 遍历所有可删除的UnifiedLog
    try {
      deletableLogs.foreach {
        case (topicPartition, log) =>
          debug(s"Garbage collecting '${log.name}'")
          // 执行删除OldSegments
          total += log.deleteOldSegments()

          val futureLog = futureLogs.get(topicPartition)
          if (futureLog != null) {
            // clean future logs
            debug(s"Garbage collecting future log '${futureLog.name}'")
            total += futureLog.deleteOldSegments()
          }
      }
    } finally {
      if (cleaner != null) {
        cleaner.resumeCleaning(deletableLogs.map(_._1))
      }
    }

    debug(s"Log cleanup completed. $total files deleted in " +
                  (time.milliseconds - startMs) / 1000 + " seconds")
  }

  /**
   * Get all the partition logs
   */
  def allLogs: Iterable[UnifiedLog] = currentLogs.values ++ futureLogs.values

  def logsByTopic(topic: String): Seq[UnifiedLog] = {
    (currentLogs.toList ++ futureLogs.toList).collect {
      case (topicPartition, log) if topicPartition.topic == topic => log
    }
  }

  /**
   * Map of log dir to logs by topic and partitions in that dir
   */
  private def logsByDir: Map[String, Map[TopicPartition, UnifiedLog]] = {
    // This code is called often by checkpoint processes and is written in a way that reduces
    // allocations and CPU with many topic partitions.
    // When changing this code please measure the changes with org.apache.kafka.jmh.server.CheckpointBench
    val byDir = new mutable.AnyRefMap[String, mutable.AnyRefMap[TopicPartition, UnifiedLog]]()
    def addToDir(tp: TopicPartition, log: UnifiedLog): Unit = {
      byDir.getOrElseUpdate(log.parentDir, new mutable.AnyRefMap[TopicPartition, UnifiedLog]()).put(tp, log)
    }
    currentLogs.foreachEntry(addToDir)
    futureLogs.foreachEntry(addToDir)
    byDir
  }

  private def logsInDir(dir: File): Map[TopicPartition, UnifiedLog] = {
    logsByDir.getOrElse(dir.getAbsolutePath, Map.empty)
  }

  private def logsInDir(cachedLogsByDir: Map[String, Map[TopicPartition, UnifiedLog]],
                        dir: File): Map[TopicPartition, UnifiedLog] = {
    cachedLogsByDir.getOrElse(dir.getAbsolutePath, Map.empty)
  }

  // logDir should be an absolute path
  def isLogDirOnline(logDir: String): Boolean = {
    // The logDir should be an absolute path
    if (!logDirs.exists(_.getAbsolutePath == logDir))
      throw new LogDirNotFoundException(s"Log dir $logDir is not found in the config.")

    _liveLogDirs.contains(new File(logDir))
  }

  /**
   * Flush any log which has exceeded its flush interval and has unwritten messages.
   */
  private def flushDirtyLogs(): Unit = {
    debug("Checking for dirty logs to flush...")

    for ((topicPartition, log) <- currentLogs.toList ++ futureLogs.toList) {
      try {
        // 自上次刷新后过去了多久
        val timeSinceLastFlush = time.milliseconds - log.lastFlushTime
        debug(s"Checking if flush is needed on ${topicPartition.topic} flush interval ${log.config.flushMs}" +
              s" last flushed ${log.lastFlushTime} time since last flush: $timeSinceLastFlush")
        // 是一个topic级别的变量TopicConfig.FLUSH_MS_CONFIG，默认是Long.MAX_VALUE
        // 所以意味着不主动指定是不会走到下面的方法触发的，官方推荐的也是利用副本来实现高可用，将flush的时机交给操作系统
        if(timeSinceLastFlush >= log.config.flushMs)
          log.flush(false)
      } catch {
        case e: Throwable =>
          error(s"Error flushing topic ${topicPartition.topic}", e)
      }
    }
  }

  private def removeLogAndMetrics(logs: Pool[TopicPartition, UnifiedLog], tp: TopicPartition): Option[UnifiedLog] = {
    // 从logs: Pool[TopicPartition, UnifiedLog]移除tp
    val removedLog = logs.remove(tp)
    //
    if (removedLog != null) {
      removedLog.removeLogMetrics()
      Some(removedLog)
    } else {
      None
    }
  }
}

object LogManager {
  // lock file定义处
  val LockFileName = ".lock"

  /**
   * Wait all jobs to complete
   * @param jobs jobs
   * @param callback this will be called to handle the exception caused by each Future#get
   * @return true if all pass. Otherwise, false
   */
  private[log] def waitForAllToComplete(jobs: Seq[Future[_]], callback: Throwable => Unit): Boolean = {
    jobs.count(future => Try(future.get) match {
      case Success(_) => false
      case Failure(e) =>
        callback(e)
        true
    }) == 0
  }

  val RecoveryPointCheckpointFile = "recovery-point-offset-checkpoint"
  val LogStartOffsetCheckpointFile = "log-start-offset-checkpoint"

  // 谁调这个apply初始化了LogManager？
  // zk模式是 KafkaServer
  // raft模式是 BrokerServer
  def apply(config: KafkaConfig,
            initialOfflineDirs: Seq[String], // 没能读到meta.properties的会被认为是initialOfflineDirs
            configRepository: ConfigRepository,
            kafkaScheduler: Scheduler,
            time: Time,
            brokerTopicStats: BrokerTopicStats,
            logDirFailureChannel: LogDirFailureChannel,
            keepPartitionMetadataFile: Boolean): LogManager = {
    val defaultProps = config.extractLogConfigMap

    LogConfig.validateBrokerLogConfigValues(defaultProps, config.isRemoteLogStorageSystemEnabled)
    val defaultLogConfig = new LogConfig(defaultProps)

    // cleanerConfig配置初始化的地方
    val cleanerConfig = LogCleaner.cleanerConfig(config)

    new LogManager(logDirs = config.logDirs.map(new File(_).getAbsoluteFile),
      initialOfflineDirs = initialOfflineDirs.map(new File(_).getAbsoluteFile),
      configRepository = configRepository,
      initialDefaultConfig = defaultLogConfig,
      cleanerConfig = cleanerConfig,
      recoveryThreadsPerDataDir = config.numRecoveryThreadsPerDataDir,
      flushCheckMs = config.logFlushSchedulerIntervalMs,
      flushRecoveryOffsetCheckpointMs = config.logFlushOffsetCheckpointIntervalMs,
      flushStartOffsetCheckpointMs = config.logFlushStartOffsetCheckpointIntervalMs,
      retentionCheckMs = config.logCleanupIntervalMs,
      maxTransactionTimeoutMs = config.transactionMaxTimeoutMs,
      producerStateManagerConfig = new ProducerStateManagerConfig(config.producerIdExpirationMs, config.transactionPartitionVerificationEnable),
      producerIdExpirationCheckIntervalMs = config.producerIdExpirationCheckIntervalMs,
      scheduler = kafkaScheduler,
      brokerTopicStats = brokerTopicStats,
      logDirFailureChannel = logDirFailureChannel,
      time = time,
      keepPartitionMetadataFile = keepPartitionMetadataFile,
      interBrokerProtocolVersion = config.interBrokerProtocolVersion,
      remoteStorageSystemEnable = config.remoteLogManagerConfig.enableRemoteStorageSystem())
  }

}
