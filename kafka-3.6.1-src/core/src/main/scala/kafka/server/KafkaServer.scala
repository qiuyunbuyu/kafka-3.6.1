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

package kafka.server

import kafka.cluster.{Broker, EndPoint}
import kafka.common.{GenerateBrokerIdException, InconsistentBrokerIdException, InconsistentClusterIdException}
import kafka.controller.KafkaController
import kafka.coordinator.group.GroupCoordinatorAdapter
import kafka.coordinator.transaction.{ProducerIdManager, TransactionCoordinator}
import kafka.log.LogManager
import kafka.log.remote.RemoteLogManager
import kafka.metrics.KafkaMetricsReporter
import kafka.network.{ControlPlaneAcceptor, DataPlaneAcceptor, RequestChannel, SocketServer}
import kafka.raft.KafkaRaftManager
import kafka.security.CredentialProvider
import kafka.server.metadata.{OffsetTrackingListener, ZkConfigRepository, ZkMetadataCache}
import kafka.utils._
import kafka.zk.{AdminZkClient, BrokerInfo, KafkaZkClient}
import org.apache.kafka.clients.{ApiVersions, ManualMetadataUpdater, NetworkClient, NetworkClientUtils}
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.BrokerRegistrationRequestData.{Listener, ListenerCollection}
import org.apache.kafka.common.message.ControlledShutdownRequestData
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ControlledShutdownRequest, ControlledShutdownResponse}
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.security.{JaasContext, JaasUtils}
import org.apache.kafka.common.utils.{AppInfoParser, LogContext, Time, Utils}
import org.apache.kafka.common.{Endpoint, KafkaException, Node, TopicPartition}
import org.apache.kafka.coordinator.group.GroupCoordinator
import org.apache.kafka.metadata.{BrokerState, MetadataRecordSerde, VersionRange}
import org.apache.kafka.raft.RaftConfig
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.common.MetadataVersion._
import org.apache.kafka.server.common.{ApiMessageAndVersion, MetadataVersion}
import org.apache.kafka.server.fault.LoggingFaultHandler
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.kafka.server.util.KafkaScheduler
import org.apache.kafka.storage.internals.log.LogDirFailureChannel
import org.apache.zookeeper.client.ZKClientConfig

import java.io.{File, IOException}
import java.net.{InetAddress, SocketTimeoutException}
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.collection.{Map, Seq}
import scala.compat.java8.OptionConverters.RichOptionForJava8
import scala.jdk.CollectionConverters._

object KafkaServer {

  def zkClientConfigFromKafkaConfig(config: KafkaConfig, forceZkSslClientEnable: Boolean = false): ZKClientConfig = {
    val clientConfig = new ZKClientConfig
    if (config.zkSslClientEnable || forceZkSslClientEnable) {
      KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslClientEnableProp, "true")
      config.zkClientCnxnSocketClassName.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkClientCnxnSocketProp, _))
      config.zkSslKeyStoreLocation.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslKeyStoreLocationProp, _))
      config.zkSslKeyStorePassword.foreach(x => KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslKeyStorePasswordProp, x.value))
      config.zkSslKeyStoreType.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslKeyStoreTypeProp, _))
      config.zkSslTrustStoreLocation.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslTrustStoreLocationProp, _))
      config.zkSslTrustStorePassword.foreach(x => KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslTrustStorePasswordProp, x.value))
      config.zkSslTrustStoreType.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslTrustStoreTypeProp, _))
      KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslProtocolProp, config.ZkSslProtocol)
      config.ZkSslEnabledProtocols.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslEnabledProtocolsProp, _))
      config.ZkSslCipherSuites.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslCipherSuitesProp, _))
      KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp, config.ZkSslEndpointIdentificationAlgorithm)
      KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslCrlEnableProp, config.ZkSslCrlEnable.toString)
      KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslOcspEnableProp, config.ZkSslOcspEnable.toString)
    }
    // The zk sasl is enabled by default so it can produce false error when broker does not intend to use SASL.
    if (!JaasUtils.isZkSaslEnabled) clientConfig.setProperty(JaasUtils.ZK_SASL_CLIENT, "false")
    clientConfig
  }

  val MIN_INCREMENTAL_FETCH_SESSION_EVICTION_MS: Long = 120000
}

/**
 * Represents the lifecycle of a single Kafka broker. Handles all functionality required
 * to start up and shutdown a single Kafka node.
 */
class KafkaServer(
  val config: KafkaConfig,
  time: Time = Time.SYSTEM,
  threadNamePrefix: Option[String] = None,
  enableForwarding: Boolean = false
) extends KafkaBroker with Server {

  // mark startupComplete
  private val startupComplete = new AtomicBoolean(false)
  // mark isShuttingDown
  private val isShuttingDown = new AtomicBoolean(false)
  // mark isStartingUp
  private val isStartingUp = new AtomicBoolean(false)

  // broker state: 刚new出KafkaServer对象时是NOT_RUNNING状态
  @volatile private var _brokerState: BrokerState = BrokerState.NOT_RUNNING
  // block the main thread and wait for kafka-server to shut down
  private var shutdownLatch = new CountDownLatch(1)

  private var logContext: LogContext = _
  private val kafkaMetricsReporters: Seq[KafkaMetricsReporter] =
    KafkaMetricsReporter.startReporters(VerifiableProperties(config.originals))
  var kafkaYammerMetrics: KafkaYammerMetrics = _
  var metrics: Metrics = _

  // auth manage
  var authorizer: Option[Authorizer] = None

  // ============================== net request handle
  // handle dataPlane Request
  @volatile var dataPlaneRequestProcessor: KafkaApis = _
  // handle controlPlane Request
  var controlPlaneRequestProcessor: KafkaApis = _
  // socketServer attach network port
  @volatile var socketServer: SocketServer = _
  // thread poll to handle dataPlane Request
  var dataPlaneRequestHandlerPool: KafkaRequestHandlerPool = _
  // thread poll to handle controlPlane Request
  var controlPlaneRequestHandlerPool: KafkaRequestHandlerPool = _

  // ============================== log manage
  var logDirFailureChannel: LogDirFailureChannel = _
  @volatile private var _logManager: LogManager = _
  var remoteLogManagerOpt: Option[RemoteLogManager] = None

  // ============================== replica manage
  @volatile private var _replicaManager: ReplicaManager = _

  // ============================== topic manage
  var adminManager: ZkAdminManager = _

  // ============================== token manage
  var tokenManager: DelegationTokenManager = _

  // ============================== dynamic config manage
  var dynamicConfigHandlers: Map[String, ConfigHandler] = _
  var dynamicConfigManager: ZkConfigManager = _
  var credentialProvider: CredentialProvider = _
  var tokenCache: DelegationTokenCache = _

  // ============================== GroupCoordinator
  @volatile var groupCoordinator: GroupCoordinator = _

  // ============================== TransactionCoordinator
  var transactionCoordinator: TransactionCoordinator = _

  // ============================== KafkaController
  @volatile private var _kafkaController: KafkaController = _

  var forwardingManager: Option[ForwardingManager] = None

  var autoTopicCreationManager: AutoTopicCreationManager = _

  var clientToControllerChannelManager: BrokerToControllerChannelManager = _

  var alterPartitionManager: AlterPartitionManager = _

  // ============================== scheduled tasks manage
  var kafkaScheduler: KafkaScheduler = _

  // ============================== kraft controller nodes
  var kraftControllerNodes: Seq[Node] = _

  // ============================== metadata Cache manage
  @volatile var metadataCache: ZkMetadataCache = _

  // ============================== quota manage
  var quotaManagers: QuotaFactory.QuotaManagers = _

  // zk client
  val zkClientConfig: ZKClientConfig = KafkaServer.zkClientConfigFromKafkaConfig(config)
  private var _zkClient: KafkaZkClient = _
  private var configRepository: ZkConfigRepository = _

  // useful for matching request and response between the client and server.
  val correlationId: AtomicInteger = new AtomicInteger(0)

  // meta.properties file in logDir
  val brokerMetaPropsFile = "meta.properties"
  val brokerMetadataCheckpoints = config.logDirs.map { logDir =>
    (logDir, new BrokerMetadataCheckpoint(new File(logDir + File.separator + brokerMetaPropsFile)))
  }.toMap

  // _clusterId, can find in meta.properties
  private var _clusterId: String = _
  @volatile var _brokerTopicStats: BrokerTopicStats = _

  private var _featureChangeListener: FinalizedFeatureChangeListener = _

  val brokerFeatures: BrokerFeatures = BrokerFeatures.createEmpty()

  override def brokerState: BrokerState = _brokerState

  def clusterId: String = _clusterId

  // Visible for testing
  private[kafka] def zkClient = _zkClient

  override def brokerTopicStats = _brokerTopicStats

  private[kafka] def featureChangeListener = _featureChangeListener

  override def replicaManager: ReplicaManager = _replicaManager

  override def logManager: LogManager = _logManager

  def kafkaController: KafkaController = _kafkaController

  var lifecycleManager: BrokerLifecycleManager = _

  @volatile var brokerEpochManager: ZkBrokerEpochManager = _

  def brokerEpochSupplier(): Long = Option(brokerEpochManager).map(_.get()).getOrElse(-1)

  /**
   * Start up API for bringing up a single instance of the Kafka server.
   * Instantiates the LogManager, the SocketServer and the request handlers - KafkaRequestHandlers
   */
  override def startup(): Unit = {
    try {
      info("starting")

      // 1. state judge
      if (isShuttingDown.get)
        throw new IllegalStateException("Kafka server is still shutting down, cannot re-start!")

      if (startupComplete.get)
        return

      // 2. canStartup
      val canStartup = isStartingUp.compareAndSet(false, true)
      if (canStartup) {

        /* ========================= 2.1 update BrokerState -> 更新状态 -> STARTING =================================*/
        _brokerState = BrokerState.STARTING

        /* 2.2 setup zookeeper, 1. connect to zk Server and try to create root node in zk*/
        // 创建zkClient + zk中基本节点
        initZkClient(time)
        configRepository = new ZkConfigRepository(new AdminZkClient(zkClient))

        /* 2.3 Get or create cluster_id */
        // 在zk上创建或获取clusterId
        _clusterId = getOrGenerateClusterId(zkClient)
        info(s"Cluster ID = $clusterId")

        /* 2.4 meta.properties and check, meta.properties content  in zk mode
        //  =======================================
        //        #Mon Dec 25 07:56:04 UTC 2023
        //        cluster.id=tWgIXcafT9Wjb4TquwS89A
        //        version=0
        //        broker.id=1
        // =======================================

        // 2.4.1: load meta.properties in each LogDir[like /data1/meta.properties, /data2/meta.properties....]*/
        // 获取initialOfflineDirs的地方： 没能正确读到目录下：meta.properties的会被认为是OfflineDirs
        val (preloadedBrokerMetadataCheckpoint, initialOfflineDirs) =
          BrokerMetadataCheckpoint.getBrokerMetadataAndOfflineDirs(config.logDirs, ignoreMissing = true, kraftMode = false)

        //  2.4.2: check version in meta.properties
        // version校验
        if (preloadedBrokerMetadataCheckpoint.version != 0) {
          throw new RuntimeException(s"Found unexpected version in loaded `meta.properties`: " +
            s"$preloadedBrokerMetadataCheckpoint. Zk-based brokers only support version 0 " +
            "(which is implicit when the `version` field is missing).")
        }

        // 2.4.3 check clusterId in meta.properties
        // meta.properties中的clusterId对比zk中的clusterId
        if (preloadedBrokerMetadataCheckpoint.clusterId.isDefined && preloadedBrokerMetadataCheckpoint.clusterId.get != clusterId)
          throw new InconsistentClusterIdException(
            s"The Cluster ID $clusterId doesn't match stored clusterId ${preloadedBrokerMetadataCheckpoint.clusterId} in meta.properties. " +
            s"The broker is trying to join the wrong cluster. Configured zookeeper.connect may be wrong.")

        // 2.4.4 Generates new brokerId if enabled or reads from meta.properties based on following conditions
        // broker.id生成或校验
        config.brokerId = getOrGenerateBrokerId(preloadedBrokerMetadataCheckpoint)
        logContext = new LogContext(s"[KafkaServer id=${config.brokerId}] ")
        this.logIdent = logContext.logPrefix

        // 2.5 initialize dynamic broker configs from ZooKeeper. Any updates made after this will be applied after ZkConfigManager starts.
        config.dynamicConfig.initialize(Some(zkClient))

        // 2.6 start scheduler task
        // 定时任务：config.backgroundThreads"background.threads" 默认10
        // Scheduler任务线程池创建/配置
        kafkaScheduler = new KafkaScheduler(config.backgroundThreads)
        kafkaScheduler.startup()

        /* 2.7 create and configure metrics */
        kafkaYammerMetrics = KafkaYammerMetrics.INSTANCE
        kafkaYammerMetrics.configure(config.originals)
        metrics = Server.initializeMetrics(config, time, clusterId)

        /* register broker metrics */
        _brokerTopicStats = new BrokerTopicStats(java.util.Optional.of(config))

        /* 2.8 quota manager */
        quotaManagers = QuotaFactory.instantiate(config, metrics, time, threadNamePrefix.getOrElse(""))
        KafkaBroker.notifyClusterListeners(clusterId, kafkaMetricsReporters ++ metrics.reporters.asScala)

        /* 2.9 ================= log manager */
        /* 2.9.1 initialize logDirFailureChannel, Used to ensure that the kafka-log directory exists */
        logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size)

        /* 2.9.2 initialize LogManager, and start log manager */
        /* the entry of LogManager */
        // 重量级：但这里只是初始化了LogManager，还没有开始”loadlog“
        _logManager = LogManager(
          config,
          initialOfflineDirs, // 这里已经传入了initial OfflineDirs
          configRepository,
          kafkaScheduler,
          time,
          brokerTopicStats,
          logDirFailureChannel,
          config.usesTopicId)

        /* ========================= update BrokerState -> 更新状态 -> RECOVERY =================================*/
        _brokerState = BrokerState.RECOVERY
        // start clean, flush, check, recovery... threads
        logManager.startup(zkClient.getAllTopicsInCluster())

        /* 2.9.3 create RemoteLog Manager*/
        remoteLogManagerOpt = createRemoteLogManager()

        /* 2.10 kraft ... */
        if (config.migrationEnabled) {
          kraftControllerNodes = RaftConfig.voterConnectionsToNodes(
            RaftConfig.parseVoterConnections(config.quorumVoters)).asScala
        } else {
          kraftControllerNodes = Seq.empty
        }

        /* 2.11
          MetadataCache初始化入口
        */
        metadataCache = MetadataCache.zkMetadataCache(
          config.brokerId,
          config.interBrokerProtocolVersion,
          brokerFeatures,
          kraftControllerNodes,
          config.migrationEnabled)
        val controllerNodeProvider = new MetadataCacheControllerNodeProvider(metadataCache, config)

        /* 2.12 initialize feature change listener */
        _featureChangeListener = new FinalizedFeatureChangeListener(metadataCache, _zkClient)
        if (config.isFeatureVersioningSupported) {
          _featureChangeListener.initOrThrow(config.zkConnectionTimeoutMs)
        }

        // 2.13
        // Enable delegation token cache for all SCRAM mechanisms to simplify dynamic update.
        // This keeps the cache up-to-date if new SCRAM mechanisms are enabled dynamically.
        tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames)
        credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, tokenCache)

        // 2.14 BrokerToControllerChannelManager
        // broker -> controller之间的channel
        clientToControllerChannelManager = BrokerToControllerChannelManager(
          controllerNodeProvider = controllerNodeProvider,
          time = time,
          metrics = metrics,
          config = config,
          channelName = "forwarding",
          s"zk-broker-${config.nodeId}-",
          retryTimeoutMs = config.requestTimeoutMs.longValue
        )
        // BrokerToControllerRequestThread启动
        clientToControllerChannelManager.start()

        /* 2.15 start forwarding manager */
        var autoTopicCreationChannel = Option.empty[BrokerToControllerChannelManager]
        if (enableForwarding) {
          this.forwardingManager = Some(ForwardingManager(clientToControllerChannelManager))
          autoTopicCreationChannel = Some(clientToControllerChannelManager)
        }

        // 2.16 initialize ApiVersionManager
        val apiVersionManager = ApiVersionManager(
          ListenerType.ZK_BROKER,
          config,
          forwardingManager,
          brokerFeatures,
          metadataCache
        )

        // * 2.17
        // Create and start the socket server acceptor threads so that the bound port is known.
        // Delay starting processors until the end of the initialization sequence to ensure
        // that credentials have been loaded before processing authentications.
        //
        // Note that we allow the use of KRaft mode controller APIs when forwarding is enabled
        // so that the Envelope request is exposed. This is only used in testing currently.
        // network port
        // SocketServer初始化
        socketServer = new SocketServer(config, metrics, time, credentialProvider, apiVersionManager)

        // 2.18 Start alter partition manager based on the IBP version
        alterPartitionManager = if (config.interBrokerProtocolVersion.isAlterPartitionSupported) {
          AlterPartitionManager(
            config = config,
            metadataCache = metadataCache,
            scheduler = kafkaScheduler,
            controllerNodeProvider,
            time = time,
            metrics = metrics,
            s"zk-broker-${config.nodeId}-",
            brokerEpochSupplier = brokerEpochSupplier
          )
        } else {
          AlterPartitionManager(kafkaScheduler, time, zkClient)
        }
        alterPartitionManager.start()

        // 2.19 Start replica manager
        // broker启动时创建ReplicaManager
        _replicaManager = createReplicaManager(isShuttingDown)
        replicaManager.startup()

        // 2.20 register brokerInfo to zk and check
        val brokerInfo = createBrokerInfo
        // register: 往zk上注册/broker/id的broker信息
        val brokerEpoch = zkClient.registerBroker(brokerInfo)
        // Now that the broker is successfully registered, checkpoint its metadata
        val zkMetaProperties = ZkMetaProperties(clusterId, config.brokerId)
        checkpointBrokerMetadata(zkMetaProperties)

        /* 2.21 start token manager */
        tokenManager = new DelegationTokenManagerZk(config, tokenCache, time , zkClient)
        tokenManager.startup()

        /* 2.22 start kafka controller */
        _kafkaController = new KafkaController(config, zkClient, time, metrics, brokerInfo, brokerEpoch, tokenManager, brokerFeatures, metadataCache, threadNamePrefix)
        kafkaController.startup()

        /* 2.23 if ZK to KRaft Migration configs: zookeeper.metadata.migration.enable = true*/
        // ZK to KRaft Migration configs迁移相关
        if (config.migrationEnabled) {
          logger.info("Starting up additional components for ZooKeeper migration")
          lifecycleManager = new BrokerLifecycleManager(config,
            time,
            s"zk-broker-${config.nodeId}-",
            isZkBroker = true)

          // If the ZK broker is in migration mode, start up a RaftManager to learn about the new KRaft controller
          val kraftMetaProps = MetaProperties(zkMetaProperties.clusterId, zkMetaProperties.brokerId)
          val controllerQuorumVotersFuture = CompletableFuture.completedFuture(
            RaftConfig.parseVoterConnections(config.quorumVoters))
          val raftManager = new KafkaRaftManager[ApiMessageAndVersion](
            kraftMetaProps,
            config,
            new MetadataRecordSerde,
            KafkaRaftServer.MetadataPartition,
            KafkaRaftServer.MetadataTopicId,
            time,
            metrics,
            threadNamePrefix,
            controllerQuorumVotersFuture,
            fatalFaultHandler = new LoggingFaultHandler("raftManager", () => shutdown())
          )
          val controllerNodes = RaftConfig.voterConnectionsToNodes(controllerQuorumVotersFuture.get()).asScala
          val quorumControllerNodeProvider = RaftControllerNodeProvider(raftManager, config, controllerNodes)
          val brokerToQuorumChannelManager = BrokerToControllerChannelManager(
            controllerNodeProvider = quorumControllerNodeProvider,
            time = time,
            metrics = metrics,
            config = config,
            channelName = "quorum",
            s"zk-broker-${config.nodeId}-",
            retryTimeoutMs = config.requestTimeoutMs.longValue
          )

          val listener = new OffsetTrackingListener()
          raftManager.register(listener)
          raftManager.startup()

          val networkListeners = new ListenerCollection()
          config.effectiveAdvertisedListeners.foreach { ep =>
            networkListeners.add(new Listener().
              setHost(if (Utils.isBlank(ep.host)) InetAddress.getLocalHost.getCanonicalHostName else ep.host).
              setName(ep.listenerName.value()).
              setPort(if (ep.port == 0) socketServer.boundPort(ep.listenerName) else ep.port).
              setSecurityProtocol(ep.securityProtocol.id))
          }

          // Even though ZK brokers don't use "metadata.version" feature, we send our IBP here as part of the broker registration
          // so the KRaft controller can verify that all brokers are on the same IBP before starting the migration.
          val ibpAsFeature =
           java.util.Collections.singletonMap(MetadataVersion.FEATURE_NAME,
             VersionRange.of(config.interBrokerProtocolVersion.featureLevel(), config.interBrokerProtocolVersion.featureLevel()))

          lifecycleManager.start(
            () => listener.highestOffset,
            brokerToQuorumChannelManager,
            kraftMetaProps.clusterId,
            networkListeners,
            ibpAsFeature
          )
          logger.debug("Start RaftManager")
        }

        // Used by ZK brokers during a KRaft migration. When talking to a KRaft controller, we need to use the epoch
        // from BrokerLifecycleManager rather than ZK (via KafkaController)
        brokerEpochManager = new ZkBrokerEpochManager(metadataCache, kafkaController, Option(lifecycleManager))

        // ZkAdminManager
        adminManager = new ZkAdminManager(config, metrics, metadataCache, zkClient)

        /* 2.24 start group coordinator GroupCoordinator初始化时机*/
        // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
        groupCoordinator = GroupCoordinatorAdapter(
          config,
          replicaManager,
          Time.SYSTEM,
          metrics
        )
        // 启动 ”group-metadata-manager- 线程“
        groupCoordinator.startup(() => zkClient.getTopicPartitionCount(Topic.GROUP_METADATA_TOPIC_NAME).getOrElse(config.offsetsTopicPartitions))

        /* 2.25 create producer ids manager */
        val producerIdManager = if (config.interBrokerProtocolVersion.isAllocateProducerIdsSupported) {
          ProducerIdManager.rpc(
            config.brokerId,
            time,
            brokerEpochSupplier = brokerEpochSupplier,
            clientToControllerChannelManager
          )
        } else {
          ProducerIdManager.zk(config.brokerId, zkClient)
        }

        /* 2.26 start transaction coordinator, with a separate background thread scheduler for transaction expiration and log loading */
        // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
        transactionCoordinator = TransactionCoordinator(config, replicaManager, new KafkaScheduler(1, true, "transaction-log-manager-"),
          () => producerIdManager, metrics, metadataCache, Time.SYSTEM)
        transactionCoordinator.startup(
          () => zkClient.getTopicPartitionCount(Topic.TRANSACTION_STATE_TOPIC_NAME).getOrElse(config.transactionTopicPartitions))

        /* 2.27 start auto topic creation manager */
        this.autoTopicCreationManager = AutoTopicCreationManager(
          config,
          metadataCache,
          threadNamePrefix,
          autoTopicCreationChannel,
          Some(adminManager),
          Some(kafkaController),
          groupCoordinator,
          transactionCoordinator
        )

        /* 2.28 ACL manage
        Get the authorizer and initialize it if one is specified.*/
        authorizer = config.createNewAuthorizer()
        authorizer.foreach(_.configure(config.originals))
        val authorizerFutures: Map[Endpoint, CompletableFuture[Void]] = authorizer match {
          case Some(authZ) =>
            authZ.start(brokerInfo.broker.toServerInfo(clusterId, config)).asScala.map { case (ep, cs) =>
              ep -> cs.toCompletableFuture
            }
          case None =>
            brokerInfo.broker.endPoints.map { ep =>
              ep.toJava -> CompletableFuture.completedFuture[Void](null)
            }.toMap
        }

        /* 2.29 fetchManager */
        val fetchManager = new FetchManager(Time.SYSTEM,
          new FetchSessionCache(config.maxIncrementalFetchSessionCacheSlots,
            KafkaServer.MIN_INCREMENTAL_FETCH_SESSION_EVICTION_MS))

        // 2.30 Start RemoteLogManager before broker start serving the requests.
        remoteLogManagerOpt.foreach { rlm =>
          val listenerName = config.remoteLogManagerConfig.remoteLogMetadataManagerListenerName()
          if (listenerName != null) {
            brokerInfo.broker.endPoints
              .find(e => e.listenerName.equals(ListenerName.normalised(listenerName)))
              .orElse(throw new ConfigException(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP,
                listenerName, "Should be set as a listener name within valid broker listener name list: "
                  + brokerInfo.broker.endPoints.map(_.listenerName).mkString(",")))
              .foreach(e => rlm.onEndPointCreated(e))
          }
          rlm.startup()
        }

        /* 2.31 start processing requests */
        // 2.31.1 initialize zkSupport
        val zkSupport = ZkSupport(adminManager, kafkaController, zkClient, forwardingManager, metadataCache, brokerEpochManager)
        // 2.31.2 initialize KafkaApis
        def createKafkaApis(requestChannel: RequestChannel): KafkaApis = new KafkaApis(
          requestChannel = requestChannel,
          metadataSupport = zkSupport,
          replicaManager = replicaManager,
          groupCoordinator = groupCoordinator,
          txnCoordinator = transactionCoordinator,
          autoTopicCreationManager = autoTopicCreationManager,
          brokerId = config.brokerId,
          config = config,
          configRepository = configRepository,
          metadataCache = metadataCache,
          metrics = metrics,
          authorizer = authorizer,
          quotas = quotaManagers,
          fetchManager = fetchManager,
          brokerTopicStats = brokerTopicStats,
          clusterId = clusterId,
          time = time,
          tokenManager = tokenManager,
          apiVersionManager = apiVersionManager)

        /* 2.31.3 dataPlaneRequest handle: KafkaRequestHandlerPool*/
        dataPlaneRequestProcessor = createKafkaApis(socketServer.dataPlaneRequestChannel)
        dataPlaneRequestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.dataPlaneRequestChannel, dataPlaneRequestProcessor, time,
          config.numIoThreads, s"${DataPlaneAcceptor.MetricPrefix}RequestHandlerAvgIdlePercent", DataPlaneAcceptor.ThreadPrefix)

        /* 2.31.4 controlPlaneRequest handle: KafkaRequestHandlerPool*/
        socketServer.controlPlaneRequestChannelOpt.foreach { controlPlaneRequestChannel =>
          controlPlaneRequestProcessor = createKafkaApis(controlPlaneRequestChannel)
          controlPlaneRequestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.controlPlaneRequestChannelOpt.get, controlPlaneRequestProcessor, time,
            1, s"${ControlPlaneAcceptor.MetricPrefix}RequestHandlerAvgIdlePercent", ControlPlaneAcceptor.ThreadPrefix)
        }

        Mx4jLoader.maybeLoad()

        // 2.32 dynamicConfig handle
        /* 2.32.1 Add all reconfigurables for config change notification before starting config handlers */
        config.dynamicConfig.addReconfigurables(this)
        Option(logManager.cleaner).foreach(config.dynamicConfig.addBrokerReconfigurable)

        /* 2.32.2 start dynamic config manager */
        dynamicConfigHandlers = Map[String, ConfigHandler](ConfigType.Topic -> new TopicConfigHandler(replicaManager, config, quotaManagers, Some(kafkaController)),
                                                           ConfigType.Client -> new ClientIdConfigHandler(quotaManagers),
                                                           ConfigType.User -> new UserConfigHandler(quotaManagers, credentialProvider),
                                                           ConfigType.Broker -> new BrokerConfigHandler(config, quotaManagers),
                                                           ConfigType.Ip -> new IpConfigHandler(socketServer.connectionQuotas))

        // 2.32.3 Create the config manager. start listening to notifications
        dynamicConfigManager = new ZkConfigManager(zkClient, dynamicConfigHandlers)
        dynamicConfigManager.startup()

        // 2.33 kraft attach
        if (config.migrationEnabled && lifecycleManager != null) {
          lifecycleManager.initialCatchUpFuture.whenComplete { case (_, t) =>
            if (t != null) {
              fatal("Encountered an exception when waiting to catch up with KRaft metadata log", t)
              shutdown()
            } else {
              info("Finished catching up on KRaft metadata log, requesting that the KRaft controller unfence this broker")
              lifecycleManager.setReadyToUnfence()
            }
          }
        }
        // ** 2.34 socketServer do work, acceptor, processor...
        // socketServer线程启动
        socketServer.enableRequestProcessing(authorizerFutures)
        // Block here until all the authorizer futures are complete
        try {
          CompletableFuture.allOf(authorizerFutures.values.toSeq: _*).join()
        } catch {
          case t: Throwable => throw new RuntimeException("Received a fatal error while " +
            "waiting for all of the authorizer futures to be completed.", t)
        }

        // 2.35 update broker state and mark
        /* ========================= update BrokerState -> 更新状态 -> RUNNING =================================*/
        _brokerState = BrokerState.RUNNING
        // shutdownLatch -> 1
        shutdownLatch = new CountDownLatch(1)
        startupComplete.set(true)
        isStartingUp.set(false)
        AppInfoParser.registerAppInfo(Server.MetricsPrefix, config.brokerId.toString, metrics, time.milliseconds())
        info("started")
      }
    }
    catch {
      case e: Throwable =>
        fatal("Fatal error during KafkaServer startup. Prepare to shutdown", e)
        isStartingUp.set(false)
        shutdown()
        throw e
    }
  }

  protected def createRemoteLogManager(): Option[RemoteLogManager] = {
    if (config.remoteLogManagerConfig.enableRemoteStorageSystem()) {
      if(config.logDirs.size > 1) {
        throw new KafkaException("Tiered storage is not supported with multiple log dirs.");
      }

      Some(new RemoteLogManager(config.remoteLogManagerConfig, config.brokerId, config.logDirs.head, clusterId, time,
        (tp: TopicPartition) => logManager.getLog(tp).asJava,
        (tp: TopicPartition, remoteLogStartOffset: java.lang.Long) => {
          logManager.getLog(tp).foreach { log =>
            log.updateLogStartOffsetFromRemoteTier(remoteLogStartOffset)
          }
      },
        brokerTopicStats));
    } else {
      None
    }
  }
  // broker启动时创建ReplicaManager的入口
  protected def createReplicaManager(isShuttingDown: AtomicBoolean): ReplicaManager = {
    // 这是干啥的？只能猜测与事务相关
    val addPartitionsLogContext = new LogContext(s"[AddPartitionsToTxnManager broker=${config.brokerId}]")
    val addPartitionsToTxnNetworkClient: NetworkClient = NetworkUtils.buildNetworkClient("AddPartitionsManager", config, metrics, time, addPartitionsLogContext)
    val addPartitionsToTxnManager: AddPartitionsToTxnManager = new AddPartitionsToTxnManager(config, addPartitionsToTxnNetworkClient, time)

    new ReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = kafkaScheduler,
      logManager = logManager,
      remoteLogManager = remoteLogManagerOpt,
      quotaManagers = quotaManagers,
      metadataCache = metadataCache,
      logDirFailureChannel = logDirFailureChannel,
      alterPartitionManager = alterPartitionManager,
      brokerTopicStats = brokerTopicStats,
      isShuttingDown = isShuttingDown,
      zkClient = Some(zkClient),
      delayedRemoteFetchPurgatoryParam = None,
      threadNamePrefix = threadNamePrefix,
      brokerEpochSupplier = brokerEpochSupplier,
      addPartitionsToTxnManager = Some(addPartitionsToTxnManager))
  }

  /**
   * 1. 创建zk_Client
   * 2. 创建基本的zk节点
   */
  private def initZkClient(time: Time): Unit = {
    info(s"Connecting to zookeeper on ${config.zkConnect}")
    // 1. create zkClient by configs
    _zkClient = KafkaZkClient.createZkClient("Kafka server", time, config, zkClientConfig)
    // 2. Pre-create top level paths in ZK if needed.
    _zkClient.createTopLevelPaths()
  }

  /**
   * /cluster/id
   */
  private def getOrGenerateClusterId(zkClient: KafkaZkClient): String = {
    zkClient.getClusterId.getOrElse(zkClient.createOrGetClusterId(CoreUtils.generateUuidAsBase64()))
  }

  def createBrokerInfo: BrokerInfo = {
    val endPoints = config.effectiveAdvertisedListeners.map(e => s"${e.host}:${e.port}")
    zkClient.getAllBrokersInCluster.filter(_.id != config.brokerId).foreach { broker =>
      val commonEndPoints = broker.endPoints.map(e => s"${e.host}:${e.port}").intersect(endPoints)
      require(commonEndPoints.isEmpty, s"Configured end points ${commonEndPoints.mkString(",")} in" +
        s" advertised listeners are already registered by broker ${broker.id}")
    }

    val listeners = config.effectiveAdvertisedListeners.map { endpoint =>
      if (endpoint.port == 0)
        endpoint.copy(port = socketServer.boundPort(endpoint.listenerName))
      else
        endpoint
    }

    val updatedEndpoints = listeners.map(endpoint =>
      if (Utils.isBlank(endpoint.host))
        endpoint.copy(host = InetAddress.getLocalHost.getCanonicalHostName)
      else
        endpoint
    )

    val jmxPort = System.getProperty("com.sun.management.jmxremote.port", "-1").toInt

    BrokerInfo(
      Broker(config.brokerId, updatedEndpoints, config.rack, brokerFeatures.supportedFeatures),
      config.interBrokerProtocolVersion,
      jmxPort)
  }

  /**
   * Performs controlled shutdown
   * 受控关闭是由即将关闭的broker向controller发送ControlledShutdownRequest
   * 当发送完请求后，broker处于阻塞状态，controller会进行leader重选举和ISR收缩调整后，会给broker发送ControlledShutdownResoponse，
   * [ControlledShutdownResoponse无异常] 且 [shutdownSucceeded]表示broker可以关闭。
   */
  private def controlledShutdown(): Unit = {
    val socketTimeoutMs = config.controllerSocketTimeoutMs

    def doControlledShutdown(retries: Int): Boolean = {
      if (config.requiresZookeeper &&
        metadataCache.getControllerId.exists(_.isInstanceOf[KRaftCachedControllerId])) {
        info("ZkBroker currently has a KRaft controller. Controlled shutdown will be handled " +
          "through broker life cycle manager")
        return true
      }
      val metadataUpdater = new ManualMetadataUpdater()
      val networkClient = {
        val channelBuilder = ChannelBuilders.clientChannelBuilder(
          config.interBrokerSecurityProtocol,
          JaasContext.Type.SERVER,
          config,
          config.interBrokerListenerName,
          config.saslMechanismInterBrokerProtocol,
          time,
          config.saslInterBrokerHandshakeRequestEnable,
          logContext)
        val selector = new Selector(
          NetworkReceive.UNLIMITED,
          config.connectionsMaxIdleMs,
          metrics,
          time,
          "kafka-server-controlled-shutdown",
          Map.empty.asJava,
          false,
          channelBuilder,
          logContext
        )
        new NetworkClient(
          selector,
          metadataUpdater,
          config.brokerId.toString,
          1,
          0,
          0,
          Selectable.USE_DEFAULT_BUFFER_SIZE,
          Selectable.USE_DEFAULT_BUFFER_SIZE,
          config.requestTimeoutMs,
          config.connectionSetupTimeoutMs,
          config.connectionSetupTimeoutMaxMs,
          time,
          false,
          new ApiVersions,
          logContext)
      }

      var shutdownSucceeded: Boolean = false

      try {

        var remainingRetries = retries
        var prevController: Node = null
        var ioException = false

        while (!shutdownSucceeded && remainingRetries > 0) {
          remainingRetries = remainingRetries - 1

          // 1. Find the controller and establish a connection to it.
          // If the controller id or the broker registration are missing, we sleep and retry (if there are remaining retries)
          metadataCache.getControllerId match {
            case Some(controllerId: ZkCachedControllerId)  =>
              metadataCache.getAliveBrokerNode(controllerId.id, config.interBrokerListenerName) match {
                case Some(broker) =>
                  // if this is the first attempt, if the controller has changed or if an exception was thrown in a previous
                  // attempt, connect to the most recent controller
                  if (ioException || broker != prevController) {

                    ioException = false

                    if (prevController != null)
                      networkClient.close(prevController.idString)

                    prevController = broker
                    metadataUpdater.setNodes(Seq(prevController).asJava)
                  }
                case None =>
                  info(s"Broker registration for controller $controllerId is not available in the metadata cache")
              }
            case Some(_: KRaftCachedControllerId) | None =>
              info("No zk controller present in the metadata cache")
          }

          // 2. issue a controlled shutdown to the controller
          if (prevController != null) {
            try {

              if (!NetworkClientUtils.awaitReady(networkClient, prevController, time, socketTimeoutMs))
                throw new SocketTimeoutException(s"Failed to connect within $socketTimeoutMs ms")

              // send the controlled shutdown request
              val controlledShutdownApiVersion: Short =
                if (config.interBrokerProtocolVersion.isLessThan(IBP_0_9_0)) 0
                else if (config.interBrokerProtocolVersion.isLessThan(IBP_2_2_IV0)) 1
                else if (config.interBrokerProtocolVersion.isLessThan(IBP_2_4_IV1)) 2
                else 3
              // 构建ControlledShutdownRequest
              val controlledShutdownRequest = new ControlledShutdownRequest.Builder(
                  new ControlledShutdownRequestData()
                    .setBrokerId(config.brokerId)
                    .setBrokerEpoch(kafkaController.brokerEpoch),
                    controlledShutdownApiVersion)
              val request = networkClient.newClientRequest(prevController.idString, controlledShutdownRequest,
                time.milliseconds(), true)

              // 发送Request并接收到Response
              val clientResponse = NetworkClientUtils.sendAndReceive(networkClient, request, time)

              val shutdownResponse = clientResponse.responseBody.asInstanceOf[ControlledShutdownResponse]

              if (shutdownResponse.error != Errors.NONE) { // case1：异常
                info(s"Controlled shutdown request returned after ${clientResponse.requestLatencyMs}ms " +
                  s"with error ${shutdownResponse.error}")
              } else if (shutdownResponse.data.remainingPartitions.isEmpty) { // case2：remainingPartitions已经处理完了
                shutdownSucceeded = true
                info("Controlled shutdown request returned successfully " +
                  s"after ${clientResponse.requestLatencyMs}ms")
              } else { // case3：remainingPartitions还有处理完，继续循环
                info(s"Controlled shutdown request returned after ${clientResponse.requestLatencyMs}ms " +
                  s"with ${shutdownResponse.data.remainingPartitions.size} partitions remaining to move")

                if (isDebugEnabled) {
                  debug("Remaining partitions to move during controlled shutdown: " +
                    s"${shutdownResponse.data.remainingPartitions}")
                }
              }
            }
            catch {
              case ioe: IOException =>
                ioException = true
                warn("Error during controlled shutdown, possibly because leader movement took longer than the " +
                  s"configured controller.socket.timeout.ms and/or request.timeout.ms: ${ioe.getMessage}")
                // ignore and try again
            }
          }
          if (!shutdownSucceeded && remainingRetries > 0) {
            Thread.sleep(config.controlledShutdownRetryBackoffMs)
            info(s"Retrying controlled shutdown ($remainingRetries retries remaining)")
          }
        }
      }
      finally
        networkClient.close()

      shutdownSucceeded
    }

    if (startupComplete.get() && config.controlledShutdownEnable) {
      // We request the controller to do a controlled shutdown. On failure, we backoff for a configured period
      // of time and try again for a configured number of retries. If all the attempt fails, we simply force
      // the shutdown.
      info("Starting controlled shutdown")

      _brokerState = BrokerState.PENDING_CONTROLLED_SHUTDOWN

      if (config.migrationEnabled && lifecycleManager != null && metadataCache.getControllerId.exists(_.isInstanceOf[KRaftCachedControllerId])) {
        // For now we'll send the heartbeat with WantShutDown set so the KRaft controller can see a broker
        // shutting down without waiting for the heartbeat to time out.
        info("Notifying KRaft of controlled shutdown")
        lifecycleManager.beginControlledShutdown()
        try {
          lifecycleManager.controlledShutdownFuture.get(5L, TimeUnit.MINUTES)
        } catch {
          case _: TimeoutException =>
            error("Timed out waiting for the controller to approve controlled shutdown")
          case e: Throwable =>
            error("Got unexpected exception waiting for controlled shutdown future", e)
        }
      }

      /** ********* 执行Controlled shutdown，相关configuration ***********/
      // ControlledShutdownMaxRetries = 3
      // ControlledShutdownRetryBackoffMs = 5000
      // ControlledShutdownEnable = true
      val shutdownSucceeded = doControlledShutdown(config.controlledShutdownMaxRetries.intValue)

      if (!shutdownSucceeded)
        warn("Proceeding to do an unclean shutdown as all the controlled shutdown attempts failed")
    }
  }

  /**
   * Shutdown API for shutting down a single instance of the Kafka server.
   * Shuts down the LogManager, the SocketServer and the log cleaner scheduler thread
   */
  override def shutdown(): Unit = {
    try {
      info("shutting down")

      if (isStartingUp.get)
        throw new IllegalStateException("Kafka server is still starting up, cannot shut down!")

      // To ensure correct behavior under concurrent calls, we need to check `shutdownLatch` first since it gets updated
      // last in the `if` block. If the order is reversed, we could shutdown twice or leave `isShuttingDown` set to
      // `true` at the end of this method.
      if (shutdownLatch.getCount > 0 && isShuttingDown.compareAndSet(false, true)) {
        /* ========================= update BrokerState -> 更新状态 -> PENDING_CONTROLLED_SHUTDOWN =================================*/
        // 阻塞：[ControlledShutdownRequest <-> ControlledShutdownResponse]
        CoreUtils.swallow(controlledShutdown(), this)

        /* ========================= update BrokerState -> 更新状态 -> SHUTTING_DOWN =================================*/
        // 经历了上面的 ”tell controller shutdown", broker就只管停自己就好了
        _brokerState = BrokerState.SHUTTING_DOWN

        // 动态配置相关：$seqNodeRoot-event-process-thread
        if (dynamicConfigManager != null)
          CoreUtils.swallow(dynamicConfigManager.shutdown(), this)

        // 网络相关
        // Stop socket server to stop accepting any more connections and requests.
        // Socket server will be shutdown towards the end of the sequence.
        if (socketServer != null)
          CoreUtils.swallow(socketServer.stopProcessingRequests(), this)
        if (dataPlaneRequestHandlerPool != null)
          CoreUtils.swallow(dataPlaneRequestHandlerPool.shutdown(), this)
        if (controlPlaneRequestHandlerPool != null)
          CoreUtils.swallow(controlPlaneRequestHandlerPool.shutdown(), this)

        /**
         * 定时任务线程池
         * We must shutdown the scheduler early because otherwise, the scheduler could touch other
         * resources that might have been shutdown and cause exceptions.
         * For example, if we didn't shutdown the scheduler first, when LogManager was closing
         * partitions one by one, the scheduler might concurrently delete old segments due to
         * retention. However, the old segments could have been closed by the LogManager, which would
         * cause an IOException and subsequently mark logdir as offline. As a result, the broker would
         * not flush the remaining partitions or write the clean shutdown marker. Ultimately, the
         * broker would have to take hours to recover the log during restart.
         */
        if (kafkaScheduler != null)
          CoreUtils.swallow(kafkaScheduler.shutdown(), this)

        // 网络相关，为啥Processor线程要放在Scheduler后面关闭
        if (dataPlaneRequestProcessor != null)
          CoreUtils.swallow(dataPlaneRequestProcessor.close(), this)
        if (controlPlaneRequestProcessor != null)
          CoreUtils.swallow(controlPlaneRequestProcessor.close(), this)

        // 权限校验相关
        CoreUtils.swallow(authorizer.foreach(_.close()), this)

        // ZkAdminManager专管topic相关的创建/删除，调整配置
        if (adminManager != null)
          CoreUtils.swallow(adminManager.shutdown(), this)

        // 2个Coordinator相关
        if (transactionCoordinator != null)
          CoreUtils.swallow(transactionCoordinator.shutdown(), this)
        if (groupCoordinator != null)
          CoreUtils.swallow(groupCoordinator.shutdown(), this)

        // Delegation Token相关
        if (tokenManager != null)
          CoreUtils.swallow(tokenManager.shutdown(), this)

        // ReplicaManager
        if (replicaManager != null)
          CoreUtils.swallow(replicaManager.shutdown(), this)

        // AlterPartitionManager
        if (alterPartitionManager != null)
          CoreUtils.swallow(alterPartitionManager.shutdown(), this)

        // ClientToControllerChannelManager
        if (clientToControllerChannelManager != null)
          CoreUtils.swallow(clientToControllerChannelManager.shutdown(), this)

        // LogManager
        if (logManager != null)
          CoreUtils.swallow(logManager.shutdown(), this)

        // kafkaController
        if (kafkaController != null)
          CoreUtils.swallow(kafkaController.shutdown(), this)

        // RemoteLogManager
        // Close remote log manager before stopping processing requests, to give a chance to any
        // of its underlying clients (especially in RemoteStorageManager and RemoteLogMetadataManager)
        // to close gracefully.
        CoreUtils.swallow(remoteLogManagerOpt.foreach(_.close()), this)

        // FeatureChangeListener
        if (featureChangeListener != null)
          CoreUtils.swallow(featureChangeListener.close(), this)

        // KafkaZkClient
        if (zkClient != null)
          CoreUtils.swallow(zkClient.close(), this)

        // QuotaManagers
        if (quotaManagers != null)
          CoreUtils.swallow(quotaManagers.shutdown(), this)

        // Even though socket server is stopped much earlier, controller can generate
        // response for controlled shutdown request. Shutdown server at the end to
        // avoid any failures (e.g. when metrics are recorded)
        if (socketServer != null)
          CoreUtils.swallow(socketServer.shutdown(), this)

        // Metrics & BrokerTopicStats
        if (metrics != null)
          CoreUtils.swallow(metrics.close(), this)
        if (brokerTopicStats != null)
          CoreUtils.swallow(brokerTopicStats.close(), this)

        // Clear all reconfigurable instances stored in DynamicBrokerConfig
        config.dynamicConfig.clear()

        // BrokerLifecycleManager
        if (lifecycleManager != null) {
          lifecycleManager.close()
        }

        /* ========================= update BrokerState -> 更新状态 -> NOT_RUNNING =================================*/
        _brokerState = BrokerState.NOT_RUNNING

        startupComplete.set(false)
        isShuttingDown.set(false)
        CoreUtils.swallow(AppInfoParser.unregisterAppInfo(Server.MetricsPrefix, config.brokerId.toString, metrics), this)
        // shutdownLatch - 1
        shutdownLatch.countDown()
        info("shut down completed")
      }
    }
    catch {
      case e: Throwable =>
        fatal("Fatal error during KafkaServer shutdown.", e)
        isShuttingDown.set(false)
        throw e
    }
  }

  /**
   * After calling shutdown(), use this API to wait until the shutdown is complete
   * start: shutdownLatch = 1
   *  ↓
   * Shutdown: shutdownLatch - 1
   *  ↓
   * awaitShutdown: shutdownLatch = 0 -> exit!
   */
  override def awaitShutdown(): Unit = shutdownLatch.await()

  def getLogManager: LogManager = logManager

  override def boundPort(listenerName: ListenerName): Int = socketServer.boundPort(listenerName)

  /** Return advertised listeners with the bound port (this may differ from the configured port if the latter is `0`). */
  def advertisedListeners: Seq[EndPoint] = {
    config.effectiveAdvertisedListeners.map { endPoint =>
      endPoint.copy(port = boundPort(endPoint.listenerName))
    }
  }

  /**
   * Checkpoint the BrokerMetadata to all the online log.dirs
   *
   * @param brokerMetadata
   */
  private def checkpointBrokerMetadata(brokerMetadata: ZkMetaProperties) = {
    for (logDir <- config.logDirs if logManager.isLogDirOnline(new File(logDir).getAbsolutePath)) {
      val checkpoint = brokerMetadataCheckpoints(logDir)
      try {
        checkpoint.write(brokerMetadata.toProperties)
      } catch {
        case e: IOException =>
          val dirPath = checkpoint.file.getAbsolutePath
          logDirFailureChannel.maybeAddOfflineLogDir(dirPath, s"Error while writing meta.properties to $dirPath", e)
      }
    }
  }

  /**
   * broker.id生成和校验的3种情况，说的很清楚
   * Generates new brokerId if enabled or reads from meta.properties based on following conditions
   * <ol>
   * <li> config has no broker.id provided and broker id generation is enabled, generates a broker.id based on Zookeeper's sequence
   * <li> config has broker.id and meta.properties contains broker.id if they don't match throws InconsistentBrokerIdException
   * <li> config has broker.id and there is no meta.properties file, creates new meta.properties and stores broker.id
   * <ol>
   *
   * @return The brokerId.
   */
  private def getOrGenerateBrokerId(brokerMetadata: RawMetaProperties): Int = {
    // 1. get brokerId from config
    val brokerId = config.brokerId
    // 2. check with brokerId in metadata.properties
    if (brokerId >= 0 && brokerMetadata.brokerId.exists(_ != brokerId))
      throw new InconsistentBrokerIdException(
        s"Configured broker.id $brokerId doesn't match stored broker.id ${brokerMetadata.brokerId} in meta.properties. " +
          s"If you moved your data, make sure your configured broker.id matches. " +
          s"If you intend to create a new broker, you should remove all data in your data directories (log.dirs).")
    else if (brokerMetadata.brokerId.isDefined)
      brokerMetadata.brokerId.get
    else if (brokerId < 0 && config.brokerIdGenerationEnable) { // generate a new brokerId from Zookeeper
      // 3. generate BrokerId
      generateBrokerId()
    } else
      brokerId
  }

  /**
    * Return a sequence id generated by updating the broker sequence id path in ZK.
    * Users can provide brokerId in the config. To avoid conflicts between ZK generated
    * sequence id and configured brokerId, we increment the generated sequence id by KafkaConfig.MaxReservedBrokerId.
    */
  private def generateBrokerId(): Int = {
    try {
      zkClient.generateBrokerSequenceId() + config.maxReservedBrokerId
    } catch {
      case e: Exception =>
        error("Failed to generate broker.id due to ", e)
        throw new GenerateBrokerIdException("Failed to generate broker.id", e)
    }
  }
}
