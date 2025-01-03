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
package kafka.controller

import com.yammer.metrics.core.Timer

import java.util.concurrent.TimeUnit
import kafka.api._
import kafka.common._
import kafka.cluster.Broker
import kafka.controller.KafkaController.{ActiveBrokerCountMetricName, ActiveControllerCountMetricName, AlterReassignmentsCallback, ControllerStateMetricName, ElectLeadersCallback, FencedBrokerCountMetricName, GlobalPartitionCountMetricName, GlobalTopicCountMetricName, ListReassignmentsCallback, OfflinePartitionsCountMetricName, PreferredReplicaImbalanceCountMetricName, ReplicasIneligibleToDeleteCountMetricName, ReplicasToDeleteCountMetricName, TopicsIneligibleToDeleteCountMetricName, TopicsToDeleteCountMetricName, UpdateFeaturesCallback, ZkMigrationStateMetricName}
import kafka.coordinator.transaction.ZkProducerIdManager
import kafka.server._
import kafka.server.metadata.ZkFinalizedFeatureCache
import kafka.utils._
import kafka.utils.Implicits._
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import kafka.zk.TopicZNode.TopicIdReplicaAssignment
import kafka.zk.{FeatureZNodeStatus, _}
import kafka.zookeeper.{StateChangeHandler, ZNodeChangeHandler, ZNodeChildChangeHandler}
import org.apache.kafka.clients.admin.FeatureUpdate.UpgradeType
import org.apache.kafka.common.ElectionType
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.{BrokerNotAvailableException, ControllerMovedException, StaleBrokerEpochException}
import org.apache.kafka.common.message.{AllocateProducerIdsRequestData, AllocateProducerIdsResponseData, AlterPartitionRequestData, AlterPartitionResponseData}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractControlRequest, ApiError, LeaderAndIsrResponse, UpdateFeaturesRequest, UpdateMetadataResponse}
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.metadata.LeaderRecoveryState
import org.apache.kafka.metadata.migration.ZkMigrationState
import org.apache.kafka.server.common.{AdminOperationException, ProducerIdsBlock}
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.util.KafkaScheduler
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.Code

import scala.collection.{Map, Seq, Set, immutable, mutable}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

sealed trait ElectionTrigger
final case object AutoTriggered extends ElectionTrigger
final case object ZkTriggered extends ElectionTrigger
final case object AdminClientTriggered extends ElectionTrigger

object KafkaController extends Logging {
  val InitialControllerEpoch = 0
  val InitialControllerEpochZkVersion = 0

  type ElectLeadersCallback = Map[TopicPartition, Either[ApiError, Int]] => Unit
  type ListReassignmentsCallback = Either[Map[TopicPartition, ReplicaAssignment], ApiError] => Unit
  type AlterReassignmentsCallback = Either[Map[TopicPartition, ApiError], ApiError] => Unit
  type UpdateFeaturesCallback = Either[ApiError, Map[String, ApiError]] => Unit

  private val ActiveControllerCountMetricName = "ActiveControllerCount"
  private val OfflinePartitionsCountMetricName = "OfflinePartitionsCount"
  private val PreferredReplicaImbalanceCountMetricName = "PreferredReplicaImbalanceCount"
  private val ControllerStateMetricName = "ControllerState"
  private val GlobalTopicCountMetricName = "GlobalTopicCount"
  private val GlobalPartitionCountMetricName = "GlobalPartitionCount"
  private val TopicsToDeleteCountMetricName = "TopicsToDeleteCount"
  private val ReplicasToDeleteCountMetricName = "ReplicasToDeleteCount"
  private val TopicsIneligibleToDeleteCountMetricName = "TopicsIneligibleToDeleteCount"
  private val ReplicasIneligibleToDeleteCountMetricName = "ReplicasIneligibleToDeleteCount"
  private val ActiveBrokerCountMetricName = "ActiveBrokerCount"
  private val FencedBrokerCountMetricName = "FencedBrokerCount"
  private val ZkMigrationStateMetricName = "ZkMigrationState"

  // package private for testing
  private[controller] val MetricNames = Set(
    ZkMigrationStateMetricName,
    ActiveControllerCountMetricName,
    OfflinePartitionsCountMetricName,
    PreferredReplicaImbalanceCountMetricName,
    ControllerStateMetricName,
    GlobalTopicCountMetricName,
    GlobalPartitionCountMetricName,
    TopicsToDeleteCountMetricName,
    ReplicasToDeleteCountMetricName,
    TopicsIneligibleToDeleteCountMetricName,
    ReplicasIneligibleToDeleteCountMetricName,
    ActiveBrokerCountMetricName,
    FencedBrokerCountMetricName
  )
}

/**
 * @param config: all broker configs
 * @param zkClient: controller user to connect with zk
 * @param time: time util
 * @param metrics: metric util
 * @param initialBrokerInfo: broker info ,like hostname, port...
 * @param initialBrokerEpoch: broker epoch,
 * @param tokenManager: delegation token manage utill
 * @param brokerFeatures
 * @param featureCache
 * @param threadNamePrefix: thread name prefix
 */
class KafkaController(val config: KafkaConfig,
                      zkClient: KafkaZkClient,
                      time: Time,
                      metrics: Metrics,
                      initialBrokerInfo: BrokerInfo,
                      initialBrokerEpoch: Long,
                      tokenManager: DelegationTokenManager,
                      brokerFeatures: BrokerFeatures,
                      featureCache: ZkFinalizedFeatureCache,
                      threadNamePrefix: Option[String] = None)
  extends ControllerEventProcessor with Logging {

  private val metricsGroup = new KafkaMetricsGroup(this.getClass)
  // log record, contain the brokerId
  this.logIdent = s"[Controller id=${config.brokerId}] "

  // broker metadata
  @volatile private var brokerInfo = initialBrokerInfo
  @volatile private var _brokerEpoch = initialBrokerEpoch

  // KIP-497相关的：KIP-500下的一个子KIP，AlterIsrRequest相关，具体可以查看对应KIP
  private val isAlterPartitionEnabled = config.interBrokerProtocolVersion.isAlterPartitionSupported

  // help for record state change
  private val stateChangeLogger = new StateChangeLogger(config.brokerId, inControllerContext = true, None)

  // controller上下文信息
  val controllerContext = new ControllerContext

  // ** 与Broker通信的能力
  // Used to create and maintain network connections
  var controllerChannelManager = new ControllerChannelManager(
    () => controllerContext.epoch,
    config,
    time,
    metrics,
    stateChangeLogger,
    threadNamePrefix
  )

  // have a separate scheduler for the controller to be able to start and stop independently of the kafka server
  // such as used for Leader Election
  // visible for testing
  private[controller] val kafkaScheduler = new KafkaScheduler(1)

  // 监听->处理事件的能力
  // controller-event-thread | 用于处理controller相关event的
  // Event Manager, Event like "add/delete.." topic
  // ** Processing flow
  // 1. KafkaController will watch the change of zk node
  // 2. put the event to "ControllerEventManager" queue
  // 3. "event handling thread" in KafkaController will get event from queue
  // 4. event handling thread call "process()" to handle event
  // visible for testing
  private[controller] val eventManager = new ControllerEventManager(config.brokerId, this, time,
    controllerContext.stats.rateAndTimeMetrics)

  // Batch processing of requests from brokers
  // 包了 controllerChannelManager 与 eventManager
  private val brokerRequestBatch = new ControllerBrokerRequestBatch(config, controllerChannelManager,
    eventManager, controllerContext, stateChangeLogger)

  // 抽象/管理-分布式数据的能力: Topic - Partition - Replica
  // replica State Machine: used for replica state transitions, like handle the change of ISR set
  val replicaStateMachine: ReplicaStateMachine = new ZkReplicaStateMachine(config, stateChangeLogger, controllerContext, zkClient,
    new ControllerBrokerRequestBatch(config, controllerChannelManager, eventManager, controllerContext, stateChangeLogger))

  // partition State Machine: used for partition state transitions
  val partitionStateMachine: PartitionStateMachine = new ZkPartitionStateMachine(config, stateChangeLogger, controllerContext, zkClient,
    new ControllerBrokerRequestBatch(config, controllerChannelManager, eventManager, controllerContext, stateChangeLogger))

  // topic delete Manager: used for *delete topic and log* | TopicDeletionManager初始化时机在 kafkaController初始化时
  private val topicDeletionManager = new TopicDeletionManager(config, controllerContext, replicaStateMachine,
    partitionStateMachine, new ControllerDeletionClient(this, zkClient))

  // ========================= handlers: will watch the different zk node event, different zk node will binding different handler ============
  // Controller关心哪些”Change“
  // zk节点 - 变更类型 - 对应待处理事件
  // 1. controller Change
  private val controllerChangeHandler = new ControllerChangeHandler(eventManager)
  // 2. broker Change
  private val brokerChangeHandler = new BrokerChangeHandler(eventManager)
  // 3. broker Modifications
  private val brokerModificationsHandlers: mutable.Map[Int, BrokerModificationsHandler] = mutable.Map.empty
  // 4. topic Change
  private val topicChangeHandler = new TopicChangeHandler(eventManager)
  // 5. topic Deletion
  private val topicDeletionHandler = new TopicDeletionHandler(eventManager)
  // 6. partition Modifications
  private val partitionModificationsHandlers: mutable.Map[String, PartitionModificationsHandler] = mutable.Map.empty
  // 7. partition Reassignment
  private val partitionReassignmentHandler = new PartitionReassignmentHandler(eventManager)
  // 8.  preferred Replica Election
  private val preferredReplicaElectionHandler = new PreferredReplicaElectionHandler(eventManager)
  // 9. ISR Change Notification
  private val isrChangeNotificationHandler = new IsrChangeNotificationHandler(eventManager)
  // 10. logDir Event Notification
  private val logDirEventNotificationHandler = new LogDirEventNotificationHandler(eventManager)

  @volatile private var activeControllerId = -1
  @volatile private var offlinePartitionCount = 0
  @volatile private var preferredReplicaImbalanceCount = 0
  @volatile private var globalTopicCount = 0
  @volatile private var globalPartitionCount = 0
  @volatile private var topicsToDeleteCount = 0
  @volatile private var replicasToDeleteCount = 0
  @volatile private var ineligibleTopicsToDeleteCount = 0
  @volatile private var ineligibleReplicasToDeleteCount = 0
  @volatile private var activeBrokerCount = 0

  /* single-thread scheduler to clean expired tokens */
  private val tokenCleanScheduler = new KafkaScheduler(1, true, "delegation-token-cleaner")

  metricsGroup.newGauge(ZkMigrationStateMetricName, () => ZkMigrationState.ZK.value().intValue())
  metricsGroup.newGauge(ActiveControllerCountMetricName, () => if (isActive) 1 else 0)
  metricsGroup.newGauge(OfflinePartitionsCountMetricName, () => offlinePartitionCount)
  metricsGroup.newGauge(PreferredReplicaImbalanceCountMetricName, () => preferredReplicaImbalanceCount)
  metricsGroup.newGauge(ControllerStateMetricName, () => state.value)
  metricsGroup.newGauge(GlobalTopicCountMetricName, () => globalTopicCount)
  metricsGroup.newGauge(GlobalPartitionCountMetricName, () => globalPartitionCount)
  metricsGroup.newGauge(TopicsToDeleteCountMetricName, () => topicsToDeleteCount)
  metricsGroup.newGauge(ReplicasToDeleteCountMetricName, () => replicasToDeleteCount)
  metricsGroup.newGauge(TopicsIneligibleToDeleteCountMetricName, () => ineligibleTopicsToDeleteCount)
  metricsGroup.newGauge(ReplicasIneligibleToDeleteCountMetricName, () => ineligibleReplicasToDeleteCount)
  metricsGroup.newGauge(ActiveBrokerCountMetricName, () => activeBrokerCount)
  // FencedBrokerCount metric is always 0 in the ZK controller.
  metricsGroup.newGauge(FencedBrokerCountMetricName, () => 0)

  /**
   * Returns true if this broker is the current controller.
   */
  def isActive: Boolean = activeControllerId == config.brokerId

  def brokerEpoch: Long = _brokerEpoch

  def epoch: Int = controllerContext.epoch

  /**
   * Invoked when the controller module of a Kafka server is started up. This does not assume that the current broker
   * is the controller. It merely registers the session expiration listener and starts the controller leader
   * elector
   */
  def startup() = {
    // 1. register zk state change watch, used to monitor zookeeper session expiration
    zkClient.registerStateChangeHandler(new StateChangeHandler {
      override val name: String = StateChangeHandlers.ControllerHandler
      // after initialize
      override def afterInitializingSession(): Unit = {
        // register RegisterBrokerAndReelect event
        eventManager.put(RegisterBrokerAndReelect)
      }
      // before initialize
      override def beforeInitializingSession(): Unit = {
        val queuedEvent = eventManager.clearAndPut(Expire)

        // Block initialization of the new session until the expiration event is being handled,
        // which ensures that all pending events have been processed before creating the new session
        queuedEvent.awaitProcessing()
      }
    })
    // 2. kafkaController call process() method to handle "Startup" event
    // 每个broker启动都会放入Startup事件 放入Startup事件
    eventManager.put(Startup)
    eventManager.start()
  }

  /**
   * Invoked when the controller module of a Kafka server is shutting down. If the broker was the current controller,
   * it shuts down the partition and replica state machines. If not, those are a no-op. In addition to that, it also
   * shuts down the controller channel manager, if one exists (i.e. if it was the current controller)
   */
  def shutdown(): Unit = {
    try {
      eventManager.close()
      onControllerResignation()
    } finally {
      removeMetrics()
    }
  }

  /**
   * On controlled shutdown, the controller first determines the partitions that the
   * shutting down broker leads, and moves leadership of those partitions to another broker
   * that is in that partition's ISR.
   *
   * @param id Id of the broker to shutdown.
   * @param brokerEpoch The broker epoch in the controlled shutdown request
   * @return The number of partitions that the broker still leads.
   */
  def controlledShutdown(id: Int, brokerEpoch: Long, controlledShutdownCallback: Try[Set[TopicPartition]] => Unit): Unit = {
    val controlledShutdownEvent = ControlledShutdown(id, brokerEpoch, controlledShutdownCallback)
    eventManager.put(controlledShutdownEvent)
  }

  private[kafka] def updateBrokerInfo(newBrokerInfo: BrokerInfo): Unit = {
    this.brokerInfo = newBrokerInfo
    zkClient.updateBrokerInfo(newBrokerInfo)
  }

  private[kafka] def enableDefaultUncleanLeaderElection(): Unit = {
    eventManager.put(UncleanLeaderElectionEnable)
  }

  private[kafka] def enableTopicUncleanLeaderElection(topic: String): Unit = {
    if (isActive) {
      eventManager.put(TopicUncleanLeaderElectionEnable(topic))
    }
  }

  def isTopicQueuedForDeletion(topic: String): Boolean = {
    topicDeletionManager.isTopicQueuedUpForDeletion(topic)
  }

  private def state: ControllerState = eventManager.state

  /**
   * This callback is invoked by the zookeeper leader elector on electing the current broker as the new controller.
   *
   * It does the following things on the become-controller state change -
   * 1. Initializes the controller's context object that holds cache objects for current topics, live brokers and
   *    leaders for all existing partitions.
   * 2. Starts the controller's channel manager
   * 3. Starts the replica state machine
   * 4. Starts the partition state machine
   * If it encounters any unexpected exception/error while becoming controller, it resigns as the current controller.
   * This ensures another controller election will be triggered and there will always be an actively serving controller
   *
   * 处理 Controller 切换的方法
   */
  private def onControllerFailover(): Unit = {
    // 1. Used for adaptation between kafka brokers of different versions
    maybeSetupFeatureVersioning()

    info("Registering handlers")

    // 2. register zk node watch: before reading source of truth from zookeeper, register the listeners to get broker/topic callbacks
    // brokerChange Handler: /brokers/ids
    // topicChange Handler: /brokers/topics
    // topicDeletion Handler: /admin/delete_topics
    // logDirEventNotification Handler: /log_dir_event_notification
    // isrChangeNotification Handler: /isr_change_notification
    val childChangeHandlers = Seq(brokerChangeHandler, topicChangeHandler, topicDeletionHandler, logDirEventNotificationHandler,
      isrChangeNotificationHandler)
    childChangeHandlers.foreach(zkClient.registerZNodeChildChangeHandler)

    val nodeChangeHandlers = Seq(preferredReplicaElectionHandler, partitionReassignmentHandler)
    nodeChangeHandlers.foreach(zkClient.registerZNodeChangeHandlerAndCheckExistence)

    // 3. Deletes all log dir event notifications. + Deletes all isr change notifications.
    info("Deleting log dir event notifications")
    zkClient.deleteLogDirEventNotifications(controllerContext.epochZkVersion)
    info("Deleting isr change notifications")
    zkClient.deleteIsrChangeNotifications(controllerContext.epochZkVersion)

    // 4.Initializing controller context
    // 刷一把ControllerContext元数据的信息
    info("Initializing controller context")
    initializeControllerContext()

    // 5. Fetching [topicsToBeDeleted + topicsIneligibleForDeletion]
    info("Fetching topic deletions in progress")
    val (topicsToBeDeleted, topicsIneligibleForDeletion) = fetchTopicDeletionsInProgress()

    // 6. initialize topicDeletionManager to handle 5. Fetch
    info("Initializing topic deletion manager")
    topicDeletionManager.init(topicsToBeDeleted, topicsIneligibleForDeletion)

    // 7. *UpdateMetadataRequest -> to let all brokers in cluster to update metadata like(Leader, ISR...)
    // We need to send UpdateMetadataRequest after the controller context is initialized and before the state machines
    // are started. The is because brokers need to receive the list of live brokers from UpdateMetadataRequest before
    // they can process the LeaderAndIsrRequests that are generated by replicaStateMachine.startup() and
    // partitionStateMachine.startup().
    info("Sending update metadata request")
    sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set.empty)

    // 8. you are leader now, so manage the replica +  partition
    // only leader can handle replica/partition state
    // 8.1 start up replicaStateMachine
    replicaStateMachine.startup()
    // 8.2 start up partitionStateMachine
    partitionStateMachine.startup()

    // ** important log can mark controller change
    info(s"Ready to serve as the new controller with epoch $epoch")

    // 9. Initialize pending reassignments. This includes reassignments sent through /admin/reassign_partitions,
    initializePartitionReassignments()

    // 10. try to do topic delete
    topicDeletionManager.tryTopicDeletion()

    // 11. handle "pending" PreferredReplicaElections
    val pendingPreferredReplicaElections = fetchPendingPreferredReplicaElections()
    onReplicaElection(pendingPreferredReplicaElections, ElectionType.PREFERRED, ZkTriggered)

    // 12. Starting the controller scheduler
    info("Starting the controller scheduler")
    kafkaScheduler.startup()

    // 13. if config the "auto.leader.rebalance.enable", should do Auto Leader Rebalance Task
    if (config.autoLeaderRebalanceEnable) {
      scheduleAutoLeaderRebalanceTask(delay = 5, unit = TimeUnit.SECONDS)
    }

    // 14. if config tokenAuthEnabled
    if (config.tokenAuthEnabled) {
      info("starting the token expiry check scheduler")
      tokenCleanScheduler.startup()
      tokenCleanScheduler.schedule("delete-expired-tokens",
        () => tokenManager.expireTokens(),
        0L,
        config.delegationTokenExpiryCheckIntervalMs)
    }
  }

  private def createFeatureZNode(newNode: FeatureZNode): Int = {
    info(s"Creating FeatureZNode at path: ${FeatureZNode.path} with contents: $newNode")
    zkClient.createFeatureZNode(newNode)
    val (_, newVersion) = zkClient.getDataAndVersion(FeatureZNode.path)
    newVersion
  }

  private def updateFeatureZNode(updatedNode: FeatureZNode): Int = {
    info(s"Updating FeatureZNode at path: ${FeatureZNode.path} with contents: $updatedNode")
    zkClient.updateFeatureZNode(updatedNode)
  }

  /**
   * This method enables the feature versioning system (KIP-584).
   *
   * Development in Kafka (from a high level) is organized into features. Each feature is tracked by
   * a name and a range of version numbers or a version number. A feature can be of two types:
   *
   * 1. Supported feature:
   * A supported feature is represented by a name (string) and a range of versions (defined by a
   * SupportedVersionRange). It refers to a feature that a particular broker advertises support for.
   * Each broker advertises the version ranges of its own supported features in its own
   * BrokerIdZNode. The contents of the advertisement are specific to the particular broker and
   * do not represent any guarantee of a cluster-wide availability of the feature for any particular
   * range of versions.
   *
   * 2. Finalized feature:
   * A finalized feature is represented by a name (string) and a specified version level (defined
   * by a Short). Whenever the feature versioning system (KIP-584) is
   * enabled, the finalized features are stored in the cluster-wide common FeatureZNode.
   * In comparison to a supported feature, the key difference is that a finalized feature exists
   * in ZK only when it is guaranteed to be supported by any random broker in the cluster for a
   * specified range of version levels. Also, the controller is the only entity modifying the
   * information about finalized features.
   *
   * This method sets up the FeatureZNode with enabled status, which means that the finalized
   * features stored in the FeatureZNode are active. The enabled status should be written by the
   * controller to the FeatureZNode only when the broker IBP config is greater than or equal to
   * IBP_2_7_IV0.
   *
   * There are multiple cases handled here:
   *
   * 1. New cluster bootstrap:
   *    A new Kafka cluster (i.e. it is deployed first time) is almost always started with IBP config
   *    setting greater than or equal to IBP_2_7_IV0. We would like to start the cluster with all
   *    the possible supported features finalized immediately. Assuming this is the case, the
   *    controller will start up and notice that the FeatureZNode is absent in the new cluster,
   *    it will then create a FeatureZNode (with enabled status) containing the entire list of
   *    supported features as its finalized features.
   *
   * 2. Broker binary upgraded, but IBP config set to lower than IBP_2_7_IV0:
   *    Imagine there was an existing Kafka cluster with IBP config less than IBP_2_7_IV0, and the
   *    broker binary has now been upgraded to a newer version that supports the feature versioning
   *    system (KIP-584). But the IBP config is still set to lower than IBP_2_7_IV0, and may be
   *    set to a higher value later. In this case, we want to start with no finalized features and
   *    allow the user to finalize them whenever they are ready i.e. in the future whenever the
   *    user sets IBP config to be greater than or equal to IBP_2_7_IV0, then the user could start
   *    finalizing the features. This process ensures we do not enable all the possible features
   *    immediately after an upgrade, which could be harmful to Kafka.
   *    This is how we handle such a case:
   *      - Before the IBP config upgrade (i.e. IBP config set to less than IBP_2_7_IV0), the
   *        controller will start up and check if the FeatureZNode is absent.
   *        - If the node is absent, it will react by creating a FeatureZNode with disabled status
   *          and empty finalized features.
   *        - Otherwise, if a node already exists in enabled status then the controller will just
   *          flip the status to disabled and clear the finalized features.
   *      - After the IBP config upgrade (i.e. IBP config set to greater than or equal to
   *        IBP_2_7_IV0), when the controller starts up it will check if the FeatureZNode exists
   *        and whether it is disabled.
   *         - If the node is in disabled status, the controller won’t upgrade all features immediately.
   *           Instead it will just switch the FeatureZNode status to enabled status. This lets the
   *           user finalize the features later.
   *         - Otherwise, if a node already exists in enabled status then the controller will leave
   *           the node umodified.
   *
   * 3. Broker binary upgraded, with existing cluster IBP config >= IBP_2_7_IV0:
   *    Imagine there was an existing Kafka cluster with IBP config >= IBP_2_7_IV0, and the broker
   *    binary has just been upgraded to a newer version (that supports IBP config IBP_2_7_IV0 and
   *    higher). The controller will start up and find that a FeatureZNode is already present with
   *    enabled status and existing finalized features. In such a case, the controller leaves the node
   *    unmodified.
   *
   * 4. Broker downgrade:
   *    Imagine that a Kafka cluster exists already and the IBP config is greater than or equal to
   *    IBP_2_7_IV0. Then, the user decided to downgrade the cluster by setting IBP config to a
   *    value less than IBP_2_7_IV0. This means the user is also disabling the feature versioning
   *    system (KIP-584). In this case, when the controller starts up with the lower IBP config, it
   *    will switch the FeatureZNode status to disabled with empty features.
   */
  private def enableFeatureVersioning(): Unit = {
    val (mayBeFeatureZNodeBytes, version) = zkClient.getDataAndVersion(FeatureZNode.path)
    if (version == ZkVersion.UnknownVersion) {
      val newVersion = createFeatureZNode(
        FeatureZNode(config.interBrokerProtocolVersion,
          FeatureZNodeStatus.Enabled,
          brokerFeatures.defaultFinalizedFeatures
        ))
      featureCache.waitUntilFeatureEpochOrThrow(newVersion, config.zkConnectionTimeoutMs)
    } else {
      val existingFeatureZNode = FeatureZNode.decode(mayBeFeatureZNodeBytes.get)
      val newFeatures = existingFeatureZNode.status match {
        case FeatureZNodeStatus.Enabled => existingFeatureZNode.features
        case FeatureZNodeStatus.Disabled =>
          if (existingFeatureZNode.features.nonEmpty) {
            warn(s"FeatureZNode at path: ${FeatureZNode.path} with disabled status" +
                 s" contains non-empty features: ${existingFeatureZNode.features}")
          }
          Map.empty[String, Short]
      }
      val newFeatureZNode = FeatureZNode(config.interBrokerProtocolVersion, FeatureZNodeStatus.Enabled, newFeatures)
      if (!newFeatureZNode.equals(existingFeatureZNode)) {
        val newVersion = updateFeatureZNode(newFeatureZNode)
        featureCache.waitUntilFeatureEpochOrThrow(newVersion, config.zkConnectionTimeoutMs)
      }
    }
  }

  /**
   * Disables the feature versioning system (KIP-584).
   *
   * Sets up the FeatureZNode with disabled status. This status means the feature versioning system
   * (KIP-584) is disabled, and, the finalized features stored in the FeatureZNode are not relevant.
   * This status should be written by the controller to the FeatureZNode only when the broker
   * IBP config is less than IBP_2_7_IV0.
   *
   * NOTE:
   * 1. When this method returns, existing finalized features (if any) will be cleared from the
   *    FeatureZNode.
   * 2. This method, unlike enableFeatureVersioning() need not wait for the FinalizedFeatureCache
   *    to be updated, because, such updates to the cache (via FinalizedFeatureChangeListener)
   *    are disabled when IBP config is < than IBP_2_7_IV0.
   */
  private def disableFeatureVersioning(): Unit = {
    val newNode = FeatureZNode(config.interBrokerProtocolVersion, FeatureZNodeStatus.Disabled, Map.empty[String, Short])
    val (mayBeFeatureZNodeBytes, version) = zkClient.getDataAndVersion(FeatureZNode.path)
    if (version == ZkVersion.UnknownVersion) {
      createFeatureZNode(newNode)
    } else {
      val existingFeatureZNode = FeatureZNode.decode(mayBeFeatureZNodeBytes.get)
      if (existingFeatureZNode.status == FeatureZNodeStatus.Disabled &&
          existingFeatureZNode.features.nonEmpty) {
        warn(s"FeatureZNode at path: ${FeatureZNode.path} with disabled status" +
             s" contains non-empty features: ${existingFeatureZNode.features}")
      }
      if (!newNode.equals(existingFeatureZNode)) {
        updateFeatureZNode(newNode)
      }
    }
  }

  private def maybeSetupFeatureVersioning(): Unit = {
    if (config.isFeatureVersioningSupported) {
      enableFeatureVersioning()
    } else {
      disableFeatureVersioning()
    }
  }

  private def scheduleAutoLeaderRebalanceTask(delay: Long, unit: TimeUnit): Unit = {
    kafkaScheduler.scheduleOnce("auto-leader-rebalance-task",
      () => eventManager.put(AutoPreferredReplicaLeaderElection),
      unit.toMillis(delay))
  }

  /**
   * This callback is invoked by the zookeeper leader elector when the current broker resigns as the controller. This is
   * required to clean up internal controller data structures
   * controller卸任操作
   * 调用的地方还比较多，ctrl + alt + h 看吧-.-
   */
  private def onControllerResignation(): Unit = {
    debug("Resigning")
    // 1. de-register listeners
    zkClient.unregisterZNodeChildChangeHandler(isrChangeNotificationHandler.path)
    zkClient.unregisterZNodeChangeHandler(partitionReassignmentHandler.path)
    zkClient.unregisterZNodeChangeHandler(preferredReplicaElectionHandler.path)
    zkClient.unregisterZNodeChildChangeHandler(logDirEventNotificationHandler.path)
    unregisterBrokerModificationsHandler(brokerModificationsHandlers.keySet)

    // 2. shutdown leader rebalance scheduler
    kafkaScheduler.shutdown()

    // 3. stop token expiry check scheduler
    tokenCleanScheduler.shutdown()

    // 4. de-register partition ISR listener for on-going partition reassignment task
    unregisterPartitionReassignmentIsrChangeHandlers()

    // 5. shutdown partition state machine
    partitionStateMachine.shutdown()

    // 6. unregister "${BrokersZNode.path}/topics"
    zkClient.unregisterZNodeChildChangeHandler(topicChangeHandler.path)

    // 7. unregister Partition Modifications Handlers
    unregisterPartitionModificationsHandlers(partitionModificationsHandlers.keys.toSeq)

    // 8. unregister "${AdminZNode.path}/delete_topics"
    zkClient.unregisterZNodeChildChangeHandler(topicDeletionHandler.path)

    // 9. shutdown replica state machine
    replicaStateMachine.shutdown()

    // 10. unregister "${BrokersZNode.path}/ids"
    zkClient.unregisterZNodeChildChangeHandler(brokerChangeHandler.path)

    // 11. controllerChannelManager shutdown
    controllerChannelManager.shutdown()

    // 12. Clear cluster metadata
    controllerContext.resetContext()

    info("Resigned")
  }

  private def removeMetrics(): Unit = {
    KafkaController.MetricNames.foreach(metricsGroup.removeMetric)
  }

  /*
   * This callback is invoked by the controller's LogDirEventNotificationListener with the list of broker ids who
   * have experienced new log directory failures. In response the controller should send LeaderAndIsrRequest
   * to all these brokers to query the state of their replicas. Replicas with an offline log directory respond with
   * KAFKA_STORAGE_ERROR, which will be handled by the LeaderAndIsrResponseReceived event.
   */
  private def onBrokerLogDirFailure(brokerIds: Seq[Int]): Unit = {
    // send LeaderAndIsrRequest for all replicas on those brokers to see if they are still online.
    info(s"Handling log directory failure for brokers ${brokerIds.mkString(",")}")
    val replicasOnBrokers = controllerContext.replicasOnBrokers(brokerIds.toSet)
    replicaStateMachine.handleStateChanges(replicasOnBrokers.toSeq, OnlineReplica)
  }

  /**
   * This callback is invoked by the replica state machine's broker change listener, with the list of newly started
   * brokers as input. It does the following -
   * 1. Sends update metadata request to all live and shutting down brokers
   * 2. Triggers the OnlinePartition state change for all new/offline partitions
   * 3. It checks whether there are reassigned replicas assigned to any newly started brokers. If
   *    so, it performs the reassignment logic for each topic/partition.
   *
   * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point for two reasons:
   * 1. The partition state machine, when triggering online state change, will refresh leader and ISR for only those
   *    partitions currently new or offline (rather than every partition this controller is aware of)
   * 2. Even if we do refresh the cache, there is no guarantee that by the time the leader and ISR request reaches
   *    every broker that it is still valid.  Brokers check the leader epoch to determine validity of the request.
   */
  private def onBrokerStartup(newBrokers: Seq[Int]): Unit = {
    info(s"New broker startup callback for ${newBrokers.mkString(",")}")
    // 1. 移除原controllerContext中维护的此Broker ID 对应的replica信息
    newBrokers.foreach(controllerContext.replicasOnOfflineDirs.remove)

    val newBrokersSet = newBrokers.toSet
    val existingBrokers = controllerContext.liveOrShuttingDownBrokerIds.diff(newBrokersSet)

    // UpdateMetadataRequest -> existingBrokers
    // 2. send to "all the existing brokers in the cluster" -> "know about the new brokers"
    // Send update metadata request to all the existing brokers in the cluster so that they know about the new brokers
    // via this update. No need to include any partition states in the request since there are no partition state changes.
    sendUpdateMetadataRequest(existingBrokers.toSeq, Set.empty)

    // UpdateMetadataRequest -> existingBrokers
    // 3. send to "all the new brokers in the cluster" -> "a full set of partition states for initialization"
    // Send update metadata request to all the new brokers in the cluster with a full set of partition states for initialization.
    // In cases of controlled shutdown leaders will not be elected when a new broker comes up. So at least in the
    // common controlled shutdown case, the metadata will reach the new brokers faster.
    sendUpdateMetadataRequest(newBrokers, controllerContext.partitionsWithLeaders)

    // 4. "first thing":
    //    1. 告诉新上来的broker所有其应该托管的TopicPartition列表
    //    2. 以上述TopicPartition列表为输入，启动 “high watermark threads”
    // the very first thing to do when a new broker comes up is send it the entire list of partitions that it is
    // supposed to host. Based on that the broker starts the high watermark threads for the input list of partitions
    val allReplicasOnNewBrokers = controllerContext.replicasOnBrokers(newBrokersSet)
    // 副本状态机：设置new broker上所有Replica为可用状态
    // 获取当前节点分配的所有的 Replica 列表，并将其状态转移为 OnlineReplica 状态
    replicaStateMachine.handleStateChanges(allReplicasOnNewBrokers.toSeq, OnlineReplica)

    // Partition状态机
    // 触发 PartitionStateMachine 的 triggerOnlinePartitionStateChange() 方法，为所有处于 NewPartition/OfflinePartition 状态的 Partition 进行 leader 选举
    // 如果 leader 选举成功，那么该 Partition 的状态就会转移到 OnlinePartition 状态，否则状态转移失败
    // when a new broker comes up, the controller needs to trigger leader election for all new and offline partitions
    // to see if these brokers can become leaders for some/all of those
    partitionStateMachine.triggerOnlinePartitionStateChange()

    // 重启Reassign任务: 如果副本迁移中有新的 Replica 落在这台新上线的节点上，那么开始执行副本迁移操作
    // check if reassignment of some partitions need to be restarted
    maybeResumeReassignments { (_, assignment) =>
      assignment.targetReplicas.exists(newBrokersSet.contains)
    }

    // 重启topic删除任务: 如果之前由于这个 Topic 设置为删除标志，但是由于其中有 Replica 掉线而导致无法删除，这里在节点启动后，尝试重新执行删除操作
    // check if topic deletion needs to be resumed. If at least one replica that belongs to the topic being deleted exists
    // on the newly restarted brokers, there is a chance that topic deletion can resume
    val replicasForTopicsToBeDeleted = allReplicasOnNewBrokers.filter(p => topicDeletionManager.isTopicQueuedUpForDeletion(p.topic))
    if (replicasForTopicsToBeDeleted.nonEmpty) {
      info(s"Some replicas ${replicasForTopicsToBeDeleted.mkString(",")} for topics scheduled for deletion " +
        s"${controllerContext.topicsToBeDeleted.mkString(",")} are on the newly restarted brokers " +
        s"${newBrokers.mkString(",")}. Signaling restart of topic deletion for these topics")
      topicDeletionManager.resumeDeletionForTopics(replicasForTopicsToBeDeleted.map(_.topic))
    }

    // 7. register BrokerModificationsHandler for newBrokers
    registerBrokerModificationsHandler(newBrokers)
  }

  private def maybeResumeReassignments(shouldResume: (TopicPartition, ReplicaAssignment) => Boolean): Unit = {
    controllerContext.partitionsBeingReassigned.foreach { tp =>
      val currentAssignment = controllerContext.partitionFullReplicaAssignment(tp)
      if (shouldResume(tp, currentAssignment))
        onPartitionReassignment(tp, currentAssignment)
    }
  }

  private def registerBrokerModificationsHandler(brokerIds: Iterable[Int]): Unit = {
    debug(s"Register BrokerModifications handler for $brokerIds")
    brokerIds.foreach { brokerId =>
      val brokerModificationsHandler = new BrokerModificationsHandler(eventManager, brokerId)
      zkClient.registerZNodeChangeHandlerAndCheckExistence(brokerModificationsHandler)
      brokerModificationsHandlers.put(brokerId, brokerModificationsHandler)
    }
  }

  private def unregisterBrokerModificationsHandler(brokerIds: Iterable[Int]): Unit = {
    debug(s"Unregister BrokerModifications handler for $brokerIds")
    // stop watch the ${BrokerIdsZNode.path}/$id
    brokerIds.foreach { brokerId =>
      brokerModificationsHandlers.remove(brokerId).foreach(handler => zkClient.unregisterZNodeChangeHandler(handler.path))
    }
  }

  /*
   * This callback is invoked by the replica state machine's broker change listener with the list of failed brokers
   * as input. It will call onReplicaBecomeOffline(...) with the list of replicas on those failed brokers as input.
   */
  private def onBrokerFailure(deadBrokers: Seq[Int]): Unit = {
    info(s"Broker failure callback for ${deadBrokers.mkString(",")}")
    // 1. remove replicasOnOfflineDirs
    deadBrokers.foreach(controllerContext.replicasOnOfflineDirs.remove)

    // 2. remove shuttingDownBrokerIds
    val deadBrokersThatWereShuttingDown =
      deadBrokers.filter(id => controllerContext.shuttingDownBrokerIds.remove(id))
    if (deadBrokersThatWereShuttingDown.nonEmpty)
      info(s"Removed ${deadBrokersThatWereShuttingDown.mkString(",")} from list of shutting down brokers.")

    // 3. set all Replicas On DeadBrokers to "offline" state
    val allReplicasOnDeadBrokers = controllerContext.replicasOnBrokers(deadBrokers.toSet)
    onReplicasBecomeOffline(allReplicasOnDeadBrokers)

    // 4. remove BrokerModificationsHandler
    unregisterBrokerModificationsHandler(deadBrokers)
  }

  private def onBrokerUpdate(updatedBrokerId: Int): Unit = {
    info(s"Broker info update callback for $updatedBrokerId")
    // send UpdateMetadata Request to all brokers in cluster
    sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set.empty)
  }

  /**
    * This method marks the given replicas as offline. It does the following -
    * 1. Marks the given partitions as offline
    * 2. Triggers the OnlinePartition state change for all new/offline partitions
    * 3. Invokes the OfflineReplica state change on the input list of newly offline replicas
    * 4. If no partitions are affected then send UpdateMetadataRequest to live or shutting down brokers
    *
    * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point. This is because
    * the partition state machine will refresh our cache for us when performing leader election for all new/offline
    * partitions coming online.
    */
  private def onReplicasBecomeOffline(newOfflineReplicas: Set[PartitionAndReplica]): Unit = {
    val (newOfflineReplicasForDeletion, newOfflineReplicasNotForDeletion) =
      newOfflineReplicas.partition(p => topicDeletionManager.isTopicQueuedUpForDeletion(p.topic))

    val partitionsWithOfflineLeader = controllerContext.partitionsWithOfflineLeader

    // trigger OfflinePartition state for all partitions whose current leader is one amongst the newOfflineReplicas
    partitionStateMachine.handleStateChanges(partitionsWithOfflineLeader.toSeq, OfflinePartition)
    // trigger OnlinePartition state changes for offline or new partitions
    val onlineStateChangeResults = partitionStateMachine.triggerOnlinePartitionStateChange()
    // trigger OfflineReplica state change for those newly offline replicas
    replicaStateMachine.handleStateChanges(newOfflineReplicasNotForDeletion.toSeq, OfflineReplica)

    // fail deletion of topics that are affected by the offline replicas
    if (newOfflineReplicasForDeletion.nonEmpty) {
      // it is required to mark the respective replicas in TopicDeletionFailed state since the replica cannot be
      // deleted when its log directory is offline. This will prevent the replica from being in TopicDeletionStarted state indefinitely
      // since topic deletion cannot be retried until at least one replica is in TopicDeletionStarted state
      topicDeletionManager.failReplicaDeletion(newOfflineReplicasForDeletion)
    }

    // If no partition has changed leader or ISR, no UpdateMetadataRequest is sent through PartitionStateMachine
    // and ReplicaStateMachine. In that case, we want to send an UpdateMetadataRequest explicitly to
    // propagate the information about the new offline brokers.
    if (newOfflineReplicasNotForDeletion.isEmpty && onlineStateChangeResults.values.forall(_.isLeft)) {
      sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set.empty)
    }
  }

  /**
   * This callback is invoked by the topic change callback with the list of failed brokers as input.
   * It does the following -
   * 1. Move the newly created partitions to the NewPartition state
   * 2. Move the newly created partitions from NewPartition->OnlinePartition state
   */
  private def onNewPartitionCreation(newPartitions: Set[TopicPartition]): Unit = {
    info(s"New partition creation callback for ${newPartitions.mkString(",")}")
    // ----------------- partitionState 和 replicaStateMachine 都设置为 NEW --------------------
    // a. partitionState -> NewPartition
    partitionStateMachine.handleStateChanges(newPartitions.toSeq, NewPartition)

    // b. replicaStateState -> NewReplica
    replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions).toSeq, NewReplica)

    // ----------------- partitionState 和 replicaStateMachine 都从 NEW 往 Online转 --------------------
    // c. partitionState [ NewPartition -> OnlinePartition ]
    // 1. write zk data
    // 2. send LeaderAndIsrRequest to Brokers(the replica of TopicPartition)
    // 3. send UpdateMetadataRequest to all brokers
    partitionStateMachine.handleStateChanges(
      newPartitions.toSeq,
      OnlinePartition,
      Some(OfflinePartitionLeaderElectionStrategy(false))
    )

    // d. replicaStateState -> [ NewReplica -> OnlineReplica ]
    replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions).toSeq, OnlineReplica)
  }

  /**
   * This callback is invoked:
   * 1. By the AlterPartitionReassignments API
   * 2. By the reassigned partitions listener which is triggered when the /admin/reassign/partitions znode is created
   * 3. When an ongoing reassignment finishes - this is detected by a change in the partition's ISR znode
   * 4. Whenever a new broker comes up which is part of an ongoing reassignment
   * 5. On controller startup/failover
   *
   * Reassigning replicas for a partition goes through a few steps listed in the code.
   * RS = current assigned replica set
   * ORS = Original replica set for partition
   * TRS = Reassigned (target) replica set
   * AR = The replicas we are adding as part of this reassignment
   * RR = The replicas we are removing as part of this reassignment
   *
   * A reassignment may have up to three phases, each with its own steps:

   * Phase U (Assignment update): Regardless of the trigger, the first step is in the reassignment process
   * is to update the existing assignment state. We always update the state in Zookeeper before
   * we update memory so that it can be resumed upon controller fail-over.
   *
   *   U1. Update ZK with RS = ORS + TRS, AR = TRS - ORS, RR = ORS - TRS.
   *   U2. Update memory with RS = ORS + TRS, AR = TRS - ORS and RR = ORS - TRS
   *   U3. If we are cancelling or replacing an existing reassignment, send StopReplica to all members
   *       of AR in the original reassignment if they are not in TRS from the new assignment
   *
   * To complete the reassignment, we need to bring the new replicas into sync, so depending on the state
   * of the ISR, we will execute one of the following steps.
   *
   * Phase A (when TRS != ISR): The reassignment is not yet complete
   *
   *   A1. Bump the leader epoch for the partition and send LeaderAndIsr updates to RS.
   *   A2. Start new replicas AR by moving replicas in AR to NewReplica state.
   *
   * Phase B (when TRS = ISR): The reassignment is complete
   *
   *   B1. Move all replicas in AR to OnlineReplica state.
   *   B2. Set RS = TRS, AR = [], RR = [] in memory.
   *   B3. Send a LeaderAndIsr request with RS = TRS. This will prevent the leader from adding any replica in TRS - ORS back in the isr.
   *       If the current leader is not in TRS or isn't alive, we move the leader to a new replica in TRS.
   *       We may send the LeaderAndIsr to more than the TRS replicas due to the
   *       way the partition state machine works (it reads replicas from ZK)
   *   B4. Move all replicas in RR to OfflineReplica state. As part of OfflineReplica state change, we shrink the
   *       isr to remove RR in ZooKeeper and send a LeaderAndIsr ONLY to the Leader to notify it of the shrunk isr.
   *       After that, we send a StopReplica (delete = false) to the replicas in RR.
   *   B5. Move all replicas in RR to NonExistentReplica state. This will send a StopReplica (delete = true) to
   *       the replicas in RR to physically delete the replicas on disk.
   *   B6. Update ZK with RS=TRS, AR=[], RR=[].
   *   B7. Remove the ISR reassign listener and maybe update the /admin/reassign_partitions path in ZK to remove this partition from it if present.
   *   B8. After electing leader, the replicas and isr information changes. So resend the update metadata request to every broker.
   *
   * In general, there are two goals we want to aim for:
   * 1. Every replica present in the replica set of a LeaderAndIsrRequest gets the request sent to it
   * 2. Replicas that are removed from a partition's assignment get StopReplica sent to them
   *
   * For example, if ORS = {1,2,3} and TRS = {4,5,6}, the values in the topic and leader/isr paths in ZK
   * may go through the following transitions.
   * RS                AR          RR          leader     isr
   * {1,2,3}           {}          {}          1          {1,2,3}           (initial state)
   * {4,5,6,1,2,3}     {4,5,6}     {1,2,3}     1          {1,2,3}           (step A2)
   * {4,5,6,1,2,3}     {4,5,6}     {1,2,3}     1          {1,2,3,4,5,6}     (phase B)
   * {4,5,6,1,2,3}     {4,5,6}     {1,2,3}     4          {1,2,3,4,5,6}     (step B3)
   * {4,5,6,1,2,3}     {4,5,6}     {1,2,3}     4          {4,5,6}           (step B4)
   * {4,5,6}           {}          {}          4          {4,5,6}           (step B6)
   *
   * Note that we have to update RS in ZK with TRS last since it's the only place where we store ORS persistently.
   * This way, if the controller crashes before that step, we can still recover.
   */
  private def onPartitionReassignment(topicPartition: TopicPartition, reassignment: ReplicaAssignment): Unit = {
    // While a reassignment is in progress, deletion is not allowed
    topicDeletionManager.markTopicIneligibleForDeletion(Set(topicPartition.topic), reason = "topic reassignment in progress")

    updateCurrentReassignment(topicPartition, reassignment)

    val addingReplicas = reassignment.addingReplicas
    val removingReplicas = reassignment.removingReplicas

    if (!isReassignmentComplete(topicPartition, reassignment)) {
      // A1. Send LeaderAndIsr request to every replica in ORS + TRS (with the new RS, AR and RR).
      updateLeaderEpochAndSendRequest(topicPartition, reassignment)
      // A2. replicas in AR -> NewReplica
      startNewReplicasForReassignedPartition(topicPartition, addingReplicas)
    } else {
      // B1. replicas in AR -> OnlineReplica
      replicaStateMachine.handleStateChanges(addingReplicas.map(PartitionAndReplica(topicPartition, _)), OnlineReplica)
      // B2. Set RS = TRS, AR = [], RR = [] in memory.
      val completedReassignment = ReplicaAssignment(reassignment.targetReplicas)
      controllerContext.updatePartitionFullReplicaAssignment(topicPartition, completedReassignment)
      // B3. Send LeaderAndIsr request with a potential new leader (if current leader not in TRS) and
      //   a new RS (using TRS) and same isr to every broker in ORS + TRS or TRS
      moveReassignedPartitionLeaderIfRequired(topicPartition, completedReassignment)
      // B4. replicas in RR -> Offline (force those replicas out of isr)
      // B5. replicas in RR -> NonExistentReplica (force those replicas to be deleted)
      stopRemovedReplicasOfReassignedPartition(topicPartition, removingReplicas)
      // B6. Update ZK with RS = TRS, AR = [], RR = [].
      updateReplicaAssignmentForPartition(topicPartition, completedReassignment)
      // B7. Remove the ISR reassign listener and maybe update the /admin/reassign_partitions path in ZK to remove this partition from it.
      removePartitionFromReassigningPartitions(topicPartition, completedReassignment)
      // B8. After electing a leader in B3, the replicas and isr information changes, so resend the update metadata request to every broker
      sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(topicPartition))
      // signal delete topic thread if reassignment for some partitions belonging to topics being deleted just completed
      topicDeletionManager.resumeDeletionForTopics(Set(topicPartition.topic))
    }
  }

  /**
   * Update the current assignment state in Zookeeper and in memory. If a reassignment is already in
   * progress, then the new reassignment will supplant it and some replicas will be shutdown.
   *
   * Note that due to the way we compute the original replica set, we cannot guarantee that a
   * cancellation will restore the original replica order. Target replicas are always listed
   * first in the replica set in the desired order, which means we have no way to get to the
   * original order if the reassignment overlaps with the current assignment. For example,
   * with an initial assignment of [1, 2, 3] and a reassignment of [3, 4, 2], then the replicas
   * will be encoded as [3, 4, 2, 1] while the reassignment is in progress. If the reassignment
   * is cancelled, there is no way to restore the original order.
   *
   * @param topicPartition The reassigning partition
   * @param reassignment The new reassignment
   */
  private def updateCurrentReassignment(topicPartition: TopicPartition, reassignment: ReplicaAssignment): Unit = {
    val currentAssignment = controllerContext.partitionFullReplicaAssignment(topicPartition)

    if (currentAssignment != reassignment) {
      debug(s"Updating assignment of partition $topicPartition from $currentAssignment to $reassignment")

      // U1. Update assignment state in zookeeper
      updateReplicaAssignmentForPartition(topicPartition, reassignment)
      // U2. Update assignment state in memory
      controllerContext.updatePartitionFullReplicaAssignment(topicPartition, reassignment)

      // If there is a reassignment already in progress, then some of the currently adding replicas
      // may be eligible for immediate removal, in which case we need to stop the replicas.
      val unneededReplicas = currentAssignment.replicas.diff(reassignment.replicas)
      if (unneededReplicas.nonEmpty)
        stopRemovedReplicasOfReassignedPartition(topicPartition, unneededReplicas)
    }

    if (!isAlterPartitionEnabled) {
      val reassignIsrChangeHandler = new PartitionReassignmentIsrChangeHandler(eventManager, topicPartition)
      zkClient.registerZNodeChangeHandler(reassignIsrChangeHandler)
    }

    controllerContext.partitionsBeingReassigned.add(topicPartition)
  }

  /**
   * Trigger a partition reassignment provided that the topic exists and is not being deleted.
   *
   * This is called when a reassignment is initially received either through Zookeeper or through the
   * AlterPartitionReassignments API
   *
   * The `partitionsBeingReassigned` field in the controller context will be updated by this
   * call after the reassignment completes validation and is successfully stored in the topic
   * assignment zNode.
   *
   * @param reassignments The reassignments to begin processing
   * @return A map of any errors in the reassignment. If the error is NONE for a given partition,
   *         then the reassignment was submitted successfully.
   */
  private def maybeTriggerPartitionReassignment(reassignments: Map[TopicPartition, ReplicaAssignment]): Map[TopicPartition, ApiError] = {
    reassignments.map { case (tp, reassignment) =>
      val topic = tp.topic

      val apiError = if (topicDeletionManager.isTopicQueuedUpForDeletion(topic)) {
        info(s"Skipping reassignment of $tp since the topic is currently being deleted")
        new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION, "The partition does not exist.")
      } else {
        val assignedReplicas = controllerContext.partitionReplicaAssignment(tp)
        if (assignedReplicas.nonEmpty) {
          try {
            onPartitionReassignment(tp, reassignment)
            ApiError.NONE
          } catch {
            case e: ControllerMovedException =>
              info(s"Failed completing reassignment of partition $tp because controller has moved to another broker")
              throw e
            case e: Throwable =>
              error(s"Error completing reassignment of partition $tp", e)
              new ApiError(Errors.UNKNOWN_SERVER_ERROR)
          }
        } else {
          new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION, "The partition does not exist.")
        }
      }

      tp -> apiError
    }
  }

  /**
    * Attempt to elect a replica as leader for each of the given partitions.
    * @param partitions The partitions to have a new leader elected
    * @param electionType The type of election to perform
    * @param electionTrigger The reason for trigger this election
    * @return A map of failed and successful elections. The keys are the topic partitions and the corresponding values are
    *         either the exception that was thrown or new leader & ISR.
    */
  private[this] def onReplicaElection(
    partitions: Set[TopicPartition],
    electionType: ElectionType,
    electionTrigger: ElectionTrigger
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    info(s"Starting replica leader election ($electionType) for partitions ${partitions.mkString(",")} triggered by $electionTrigger")
    try {
      val strategy = electionType match {
        case ElectionType.PREFERRED => PreferredReplicaPartitionLeaderElectionStrategy
        case ElectionType.UNCLEAN =>
          /* Let's be conservative and only trigger unclean election if the election type is unclean and it was
           * triggered by the admin client
           */
          OfflinePartitionLeaderElectionStrategy(allowUnclean = electionTrigger == AdminClientTriggered)
      }

      val results = partitionStateMachine.handleStateChanges(
        partitions.toSeq,
        OnlinePartition,
        Some(strategy)
      )
      if (electionTrigger != AdminClientTriggered) {
        results.foreach {
          case (tp, Left(throwable)) =>
            if (throwable.isInstanceOf[ControllerMovedException]) {
              info(s"Error completing replica leader election ($electionType) for partition $tp because controller has moved to another broker.", throwable)
              throw throwable
            } else {
              error(s"Error completing replica leader election ($electionType) for partition $tp", throwable)
            }
          case (_, Right(_)) => // Ignored; No need to log or throw exception for the success cases
        }
      }

      results
    } finally {
      if (electionTrigger != AdminClientTriggered) {
        removePartitionsFromPreferredReplicaElection(partitions, electionTrigger == AutoTriggered)
      }
    }
  }

  private def initializeControllerContext(): Unit = {
    // update controller cache with delete topic information
    val curBrokerAndEpochs = zkClient.getAllBrokerAndEpochsInCluster
    val (compatibleBrokerAndEpochs, incompatibleBrokerAndEpochs) = partitionOnFeatureCompatibility(curBrokerAndEpochs)
    if (incompatibleBrokerAndEpochs.nonEmpty) {
      warn("Ignoring registration of new brokers due to incompatibilities with finalized features: " +
        incompatibleBrokerAndEpochs.map { case (broker, _) => broker.id }.toSeq.sorted.mkString(","))
    }
    // 更新Brokers信息
    controllerContext.setLiveBrokers(compatibleBrokerAndEpochs)
    info(s"Initialized broker epochs cache: ${controllerContext.liveBrokerIdAndEpochs}")
    // 更新Topics信息
    controllerContext.setAllTopics(zkClient.getAllTopicsInCluster(true))

    registerPartitionModificationsHandlers(controllerContext.allTopics.toSeq)
    val replicaAssignmentAndTopicIds = zkClient.getReplicaAssignmentAndTopicIdForTopics(controllerContext.allTopics.toSet)
    processTopicIds(replicaAssignmentAndTopicIds)

    replicaAssignmentAndTopicIds.foreach { case TopicIdReplicaAssignment(_, _, assignments) =>
      assignments.foreach { case (topicPartition, replicaAssignment) =>
        // 更新partition相关信息
        controllerContext.updatePartitionFullReplicaAssignment(topicPartition, replicaAssignment)
        if (replicaAssignment.isBeingReassigned)
          controllerContext.partitionsBeingReassigned.add(topicPartition)
      }
    }
    controllerContext.clearPartitionLeadershipInfo()
    controllerContext.shuttingDownBrokerIds.clear()
    // register broker modifications handlers
    registerBrokerModificationsHandler(controllerContext.liveOrShuttingDownBrokerIds)
    // update the leader and isr cache for all existing partitions from Zookeeper
    updateLeaderAndIsrCache()

    // start the channel manager
    controllerChannelManager.startup(controllerContext.liveOrShuttingDownBrokers)
    info(s"Currently active brokers in the cluster: ${controllerContext.liveBrokerIds}")
    info(s"Currently shutting brokers in the cluster: ${controllerContext.shuttingDownBrokerIds}")
    info(s"Current list of topics in the cluster: ${controllerContext.allTopics}")
  }

  private def fetchPendingPreferredReplicaElections(): Set[TopicPartition] = {
    val partitionsUndergoingPreferredReplicaElection = zkClient.getPreferredReplicaElection
    // check if they are already completed or topic was deleted
    val partitionsThatCompletedPreferredReplicaElection = partitionsUndergoingPreferredReplicaElection.filter { partition =>
      val replicas = controllerContext.partitionReplicaAssignment(partition)
      val topicDeleted = replicas.isEmpty
      val successful =
        if (!topicDeleted) controllerContext.partitionLeadershipInfo(partition).get.leaderAndIsr.leader == replicas.head else false
      successful || topicDeleted
    }
    val pendingPreferredReplicaElectionsIgnoringTopicDeletion = partitionsUndergoingPreferredReplicaElection -- partitionsThatCompletedPreferredReplicaElection
    val pendingPreferredReplicaElectionsSkippedFromTopicDeletion = pendingPreferredReplicaElectionsIgnoringTopicDeletion.filter(partition => topicDeletionManager.isTopicQueuedUpForDeletion(partition.topic))
    val pendingPreferredReplicaElections = pendingPreferredReplicaElectionsIgnoringTopicDeletion -- pendingPreferredReplicaElectionsSkippedFromTopicDeletion
    info(s"Partitions undergoing preferred replica election: ${partitionsUndergoingPreferredReplicaElection.mkString(",")}")
    info(s"Partitions that completed preferred replica election: ${partitionsThatCompletedPreferredReplicaElection.mkString(",")}")
    info(s"Skipping preferred replica election for partitions due to topic deletion: ${pendingPreferredReplicaElectionsSkippedFromTopicDeletion.mkString(",")}")
    info(s"Resuming preferred replica election for partitions: ${pendingPreferredReplicaElections.mkString(",")}")
    pendingPreferredReplicaElections
  }

  /**
   * Initialize pending reassignments. This includes reassignments sent through /admin/reassign_partitions,
   * which will supplant any API reassignments already in progress.
   */
  private def initializePartitionReassignments(): Unit = {
    // New reassignments may have been submitted through Zookeeper while the controller was failing over
    val zkPartitionsResumed = processZkPartitionReassignment()
    // We may also have some API-based reassignments that need to be restarted
    maybeResumeReassignments { (tp, _) =>
      !zkPartitionsResumed.contains(tp)
    }
  }

  private def fetchTopicDeletionsInProgress(): (Set[String], Set[String]) = {
    val topicsToBeDeleted = zkClient.getTopicDeletions.toSet
    val topicsWithOfflineReplicas = controllerContext.allTopics.filter { topic => {
      val replicasForTopic = controllerContext.replicasForTopic(topic)
      replicasForTopic.exists(r => !controllerContext.isReplicaOnline(r.replica, r.topicPartition))
    }}
    val topicsForWhichPartitionReassignmentIsInProgress = controllerContext.partitionsBeingReassigned.map(_.topic)
    val topicsIneligibleForDeletion = topicsWithOfflineReplicas | topicsForWhichPartitionReassignmentIsInProgress
    info(s"List of topics to be deleted: ${topicsToBeDeleted.mkString(",")}")
    info(s"List of topics ineligible for deletion: ${topicsIneligibleForDeletion.mkString(",")}")
    (topicsToBeDeleted, topicsIneligibleForDeletion)
  }

  private def updateLeaderAndIsrCache(partitions: Seq[TopicPartition] = controllerContext.allPartitions.toSeq): Unit = {
    val leaderIsrAndControllerEpochs = zkClient.getTopicPartitionStates(partitions)
    leaderIsrAndControllerEpochs.forKeyValue { (partition, leaderIsrAndControllerEpoch) =>
      controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)
    }
  }

  private def isReassignmentComplete(partition: TopicPartition, assignment: ReplicaAssignment): Boolean = {
    if (!assignment.isBeingReassigned) {
      true
    } else {
      zkClient.getTopicPartitionStates(Seq(partition)).get(partition).exists { leaderIsrAndControllerEpoch =>
        val isr = leaderIsrAndControllerEpoch.leaderAndIsr.isr.toSet
        val targetReplicas = assignment.targetReplicas.toSet
        targetReplicas.subsetOf(isr)
      }
    }
  }

  private def moveReassignedPartitionLeaderIfRequired(topicPartition: TopicPartition,
                                                      newAssignment: ReplicaAssignment): Unit = {
    val reassignedReplicas = newAssignment.replicas
    val currentLeader = controllerContext.partitionLeadershipInfo(topicPartition).get.leaderAndIsr.leader

    if (!reassignedReplicas.contains(currentLeader)) {
      info(s"Leader $currentLeader for partition $topicPartition being reassigned, " +
        s"is not in the new list of replicas ${reassignedReplicas.mkString(",")}. Re-electing leader")
      // move the leader to one of the alive and caught up new replicas
      partitionStateMachine.handleStateChanges(Seq(topicPartition), OnlinePartition, Some(ReassignPartitionLeaderElectionStrategy))
    } else if (controllerContext.isReplicaOnline(currentLeader, topicPartition)) {
      info(s"Leader $currentLeader for partition $topicPartition being reassigned, " +
        s"is already in the new list of replicas ${reassignedReplicas.mkString(",")} and is alive")
      // shrink replication factor and update the leader epoch in zookeeper to use on the next LeaderAndIsrRequest
      updateLeaderEpochAndSendRequest(topicPartition, newAssignment)
    } else {
      info(s"Leader $currentLeader for partition $topicPartition being reassigned, " +
        s"is already in the new list of replicas ${reassignedReplicas.mkString(",")} but is dead")
      partitionStateMachine.handleStateChanges(Seq(topicPartition), OnlinePartition, Some(ReassignPartitionLeaderElectionStrategy))
    }
  }

  private def stopRemovedReplicasOfReassignedPartition(topicPartition: TopicPartition,
                                                       removedReplicas: Seq[Int]): Unit = {
    // first move the replica to offline state (the controller removes it from the ISR)
    val replicasToBeDeleted = removedReplicas.map(PartitionAndReplica(topicPartition, _))
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, OfflineReplica)
    // send stop replica command to the old replicas
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, ReplicaDeletionStarted)
    // TODO: Eventually partition reassignment could use a callback that does retries if deletion failed
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, ReplicaDeletionSuccessful)
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, NonExistentReplica)
  }

  private def updateReplicaAssignmentForPartition(topicPartition: TopicPartition, assignment: ReplicaAssignment): Unit = {
    val topicAssignment = mutable.Map() ++=
      controllerContext.partitionFullReplicaAssignmentForTopic(topicPartition.topic) +=
      (topicPartition -> assignment)

    val setDataResponse = zkClient.setTopicAssignmentRaw(topicPartition.topic,
      controllerContext.topicIds.get(topicPartition.topic),
      topicAssignment, controllerContext.epochZkVersion)
    setDataResponse.resultCode match {
      case Code.OK =>
        info(s"Successfully updated assignment of partition $topicPartition to $assignment")
      case Code.NONODE =>
        throw new IllegalStateException(s"Failed to update assignment for $topicPartition since the topic " +
          "has no current assignment")
      case _ => throw new KafkaException(setDataResponse.resultException.get)
    }
  }

  private def startNewReplicasForReassignedPartition(topicPartition: TopicPartition, newReplicas: Seq[Int]): Unit = {
    // send the start replica request to the brokers in the reassigned replicas list that are not in the assigned
    // replicas list
    newReplicas.foreach { replica =>
      replicaStateMachine.handleStateChanges(Seq(PartitionAndReplica(topicPartition, replica)), NewReplica)
    }
  }

  private def updateLeaderEpochAndSendRequest(topicPartition: TopicPartition,
                                              assignment: ReplicaAssignment): Unit = {
    val stateChangeLog = stateChangeLogger.withControllerEpoch(controllerContext.epoch)
    updateLeaderEpoch(topicPartition) match {
      case Some(updatedLeaderIsrAndControllerEpoch) =>
        try {
          brokerRequestBatch.newBatch()
          // the isNew flag, when set to true, makes sure that when a replica possibly resided
          // in a logDir that is offline, we refrain from just creating a new replica in a good
          // logDir. This is exactly the behavior we want for the original replicas, but not
          // for the replicas we add in this reassignment. For new replicas, want to be able
          // to assign to one of the good logDirs.
          brokerRequestBatch.addLeaderAndIsrRequestForBrokers(assignment.originReplicas, topicPartition,
            updatedLeaderIsrAndControllerEpoch, assignment, isNew = false)
          brokerRequestBatch.addLeaderAndIsrRequestForBrokers(assignment.addingReplicas, topicPartition,
            updatedLeaderIsrAndControllerEpoch, assignment, isNew = true)
          brokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch)
        } catch {
          case e: IllegalStateException =>
            handleIllegalState(e)
        }
        stateChangeLog.info(s"Sent LeaderAndIsr request $updatedLeaderIsrAndControllerEpoch with " +
          s"new replica assignment $assignment to leader ${updatedLeaderIsrAndControllerEpoch.leaderAndIsr.leader} " +
          s"for partition being reassigned $topicPartition")

      case None => // fail the reassignment
        stateChangeLog.error(s"Failed to send LeaderAndIsr request with new replica assignment " +
          s"$assignment to leader for partition being reassigned $topicPartition")
    }
  }

  private def registerPartitionModificationsHandlers(topics: Seq[String]): Unit = {
    topics.foreach { topic =>
      val partitionModificationsHandler = new PartitionModificationsHandler(eventManager, topic)
      partitionModificationsHandlers.put(topic, partitionModificationsHandler)
    }
    partitionModificationsHandlers.values.foreach(zkClient.registerZNodeChangeHandler)
  }

  private[controller] def unregisterPartitionModificationsHandlers(topics: Seq[String]): Unit = {
    topics.foreach { topic =>
      partitionModificationsHandlers.remove(topic).foreach(handler => zkClient.unregisterZNodeChangeHandler(handler.path))
    }
  }

  private def unregisterPartitionReassignmentIsrChangeHandlers(): Unit = {
    if (!isAlterPartitionEnabled) {
      controllerContext.partitionsBeingReassigned.foreach { tp =>
        val path = TopicPartitionStateZNode.path(tp)
        zkClient.unregisterZNodeChangeHandler(path)
      }
    }
  }

  private def removePartitionFromReassigningPartitions(topicPartition: TopicPartition,
                                                       assignment: ReplicaAssignment): Unit = {
    if (controllerContext.partitionsBeingReassigned.contains(topicPartition)) {
      if (!isAlterPartitionEnabled) {
        val path = TopicPartitionStateZNode.path(topicPartition)
        zkClient.unregisterZNodeChangeHandler(path)
      }
      maybeRemoveFromZkReassignment((tp, replicas) => tp == topicPartition && replicas == assignment.replicas)
      controllerContext.partitionsBeingReassigned.remove(topicPartition)
    } else {
      throw new IllegalStateException("Cannot remove a reassigning partition because it is not present in memory")
    }
  }

  /**
   * Remove partitions from an active zk-based reassignment (if one exists).
   *
   * @param shouldRemoveReassignment Predicate indicating which partition reassignments should be removed
   */
  private def maybeRemoveFromZkReassignment(shouldRemoveReassignment: (TopicPartition, Seq[Int]) => Boolean): Unit = {
    if (!zkClient.reassignPartitionsInProgress)
      return

    val reassigningPartitions = zkClient.getPartitionReassignment
    val (removingPartitions, updatedPartitionsBeingReassigned) = reassigningPartitions.partition { case (tp, replicas) =>
      shouldRemoveReassignment(tp, replicas)
    }
    info(s"Removing partitions $removingPartitions from the list of reassigned partitions in zookeeper")

    // write the new list to zookeeper
    if (updatedPartitionsBeingReassigned.isEmpty) {
      info(s"No more partitions need to be reassigned. Deleting zk path ${ReassignPartitionsZNode.path}")
      zkClient.deletePartitionReassignment(controllerContext.epochZkVersion)
      // Ensure we detect future reassignments
      eventManager.put(ZkPartitionReassignment)
    } else {
      try {
        zkClient.setOrCreatePartitionReassignment(updatedPartitionsBeingReassigned, controllerContext.epochZkVersion)
      } catch {
        case e: KeeperException => throw new AdminOperationException(e)
      }
    }
  }

  private def removePartitionsFromPreferredReplicaElection(partitionsToBeRemoved: Set[TopicPartition],
                                                           isTriggeredByAutoRebalance : Boolean): Unit = {
    for (partition <- partitionsToBeRemoved) {
      // check the status
      val currentLeader = controllerContext.partitionLeadershipInfo(partition).get.leaderAndIsr.leader
      val preferredReplica = controllerContext.partitionReplicaAssignment(partition).head
      if (currentLeader == preferredReplica) {
        info(s"Partition $partition completed preferred replica leader election. New leader is $preferredReplica")
      } else {
        warn(s"Partition $partition failed to complete preferred replica leader election to $preferredReplica. " +
          s"Leader is still $currentLeader")
      }
    }
    if (!isTriggeredByAutoRebalance) {
      zkClient.deletePreferredReplicaElection(controllerContext.epochZkVersion)
      // Ensure we detect future preferred replica leader elections
      eventManager.put(ReplicaLeaderElection(None, ElectionType.PREFERRED, ZkTriggered))
    }
  }

  /**
   * Send the leader information for selected partitions to selected brokers so that they can correctly respond to
   * metadata requests
   *
   * @param brokers The brokers that the update metadata request should be sent to
   */
  private[controller] def sendUpdateMetadataRequest(brokers: Seq[Int], partitions: Set[TopicPartition]): Unit = {
    try {
      // 1. create a new RequestBatch
      brokerRequestBatch.newBatch()
      // 2. add UpdateMetadataRequest to RequestBatch
      brokerRequestBatch.addUpdateMetadataRequestForBrokers(brokers, partitions)
      // 3. send RequestBatch to brokers (bring epoch For idempotence)
      brokerRequestBatch.sendRequestsToBrokers(epoch)
    } catch {
      case e: IllegalStateException =>
        handleIllegalState(e)
    }
  }

  /**
   * Does not change leader or isr, but just increments the leader epoch
   *
   * @param partition partition
   * @return the new leaderAndIsr with an incremented leader epoch, or None if leaderAndIsr is empty.
   */
  private def updateLeaderEpoch(partition: TopicPartition): Option[LeaderIsrAndControllerEpoch] = {
    debug(s"Updating leader epoch for partition $partition")
    var finalLeaderIsrAndControllerEpoch: Option[LeaderIsrAndControllerEpoch] = None
    var zkWriteCompleteOrUnnecessary = false
    while (!zkWriteCompleteOrUnnecessary) {
      // refresh leader and isr from zookeeper again
      zkWriteCompleteOrUnnecessary = zkClient.getTopicPartitionStates(Seq(partition)).get(partition) match {
        case Some(leaderIsrAndControllerEpoch) =>
          val leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr
          val controllerEpoch = leaderIsrAndControllerEpoch.controllerEpoch
          if (controllerEpoch > epoch)
            throw new StateChangeFailedException("Leader and isr path written by another controller. This probably " +
              s"means the current controller with epoch $epoch went through a soft failure and another " +
              s"controller was elected with epoch $controllerEpoch. Aborting state change by this controller")
          // increment the leader epoch even if there are no leader or isr changes to allow the leader to cache the expanded
          // assigned replica list
          val newLeaderAndIsr = leaderAndIsr.newEpoch
          // update the new leadership decision in zookeeper or retry
          val UpdateLeaderAndIsrResult(finishedUpdates, _) =
            zkClient.updateLeaderAndIsr(immutable.Map(partition -> newLeaderAndIsr), epoch, controllerContext.epochZkVersion)

          finishedUpdates.get(partition) match {
            case Some(Right(leaderAndIsr)) =>
              val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, epoch)
              controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)
              finalLeaderIsrAndControllerEpoch = Some(leaderIsrAndControllerEpoch)
              info(s"Updated leader epoch for partition $partition to ${leaderAndIsr.leaderEpoch}, zkVersion=${leaderAndIsr.partitionEpoch}")
              true
            case Some(Left(e)) => throw e
            case None => false
          }
        case None =>
          throw new IllegalStateException(s"Cannot update leader epoch for partition $partition as " +
            "leaderAndIsr path is empty. This could mean we somehow tried to reassign a partition that doesn't exist")
      }
    }
    finalLeaderIsrAndControllerEpoch
  }

  private def checkAndTriggerAutoLeaderRebalance(): Unit = {
    trace("Checking need to trigger auto leader balancing")
    val preferredReplicasForTopicsByBrokers: Map[Int, Map[TopicPartition, Seq[Int]]] =
      controllerContext.allPartitions.filterNot {
        tp => topicDeletionManager.isTopicQueuedUpForDeletion(tp.topic)
      }.map { tp =>
        (tp, controllerContext.partitionReplicaAssignment(tp) )
      }.toMap.groupBy { case (_, assignedReplicas) => assignedReplicas.head }

    // for each broker, check if a preferred replica election needs to be triggered
    preferredReplicasForTopicsByBrokers.forKeyValue { (leaderBroker, topicPartitionsForBroker) =>
      val topicsNotInPreferredReplica = topicPartitionsForBroker.filter { case (topicPartition, _) =>
        val leadershipInfo = controllerContext.partitionLeadershipInfo(topicPartition)
        leadershipInfo.exists(_.leaderAndIsr.leader != leaderBroker)
      }
      debug(s"Topics not in preferred replica for broker $leaderBroker $topicsNotInPreferredReplica")

      val imbalanceRatio = topicsNotInPreferredReplica.size.toDouble / topicPartitionsForBroker.size
      trace(s"Leader imbalance ratio for broker $leaderBroker is $imbalanceRatio")

      // check ratio and if greater than desired ratio, trigger a rebalance for the topic partitions
      // that need to be on this broker
      if (imbalanceRatio > (config.leaderImbalancePerBrokerPercentage.toDouble / 100)) {
        val candidatePartitions = topicsNotInPreferredReplica.keys.filter(tp =>
          !topicDeletionManager.isTopicQueuedUpForDeletion(tp.topic) &&
          controllerContext.allTopics.contains(tp.topic) &&
          canPreferredReplicaBeLeader(tp)
       )
        onReplicaElection(candidatePartitions.toSet, ElectionType.PREFERRED, AutoTriggered)
      }
    }
  }

  private def canPreferredReplicaBeLeader(tp: TopicPartition): Boolean = {
    val assignment = controllerContext.partitionReplicaAssignment(tp)
    val liveReplicas = assignment.filter(replica => controllerContext.isReplicaOnline(replica, tp))
    val isr = controllerContext.partitionLeadershipInfo(tp).get.leaderAndIsr.isr
    PartitionLeaderElectionAlgorithms
      .preferredReplicaPartitionLeaderElection(assignment, isr, liveReplicas.toSet)
      .nonEmpty
  }

  private def processAutoPreferredReplicaLeaderElection(): Unit = {
    if (!isActive) return
    try {
      info("Processing automatic preferred replica leader election")
      checkAndTriggerAutoLeaderRebalance()
    } finally {
      scheduleAutoLeaderRebalanceTask(delay = config.leaderImbalanceCheckIntervalSeconds, unit = TimeUnit.SECONDS)
    }
  }

  private def processUncleanLeaderElectionEnable(): Unit = {
    if (!isActive) return
    info("Unclean leader election has been enabled by default")
    partitionStateMachine.triggerOnlinePartitionStateChange()
  }

  private def processTopicUncleanLeaderElectionEnable(topic: String): Unit = {
    if (!isActive) return
    info(s"Unclean leader election has been enabled for topic $topic")
    partitionStateMachine.triggerOnlinePartitionStateChange(topic)
  }

  private def processControlledShutdown(id: Int, brokerEpoch: Long, controlledShutdownCallback: Try[Set[TopicPartition]] => Unit): Unit = {
    val controlledShutdownResult = Try { doControlledShutdown(id, brokerEpoch) }
    controlledShutdownCallback(controlledShutdownResult)
  }

  private def doControlledShutdown(id: Int, brokerEpoch: Long): Set[TopicPartition] = {
    if (!isActive) {
      throw new ControllerMovedException("Controller moved to another broker. Aborting controlled shutdown")
    }

    // broker epoch in the request is unknown if the controller hasn't been upgraded to use KIP-380
    // so we will keep the previous behavior and don't reject the request
    if (brokerEpoch != AbstractControlRequest.UNKNOWN_BROKER_EPOCH) {
      val cachedBrokerEpoch = controllerContext.liveBrokerIdAndEpochs(id)
      if (brokerEpoch < cachedBrokerEpoch) {
        val stateBrokerEpochErrorMessage = "Received controlled shutdown request from an old broker epoch " +
          s"$brokerEpoch for broker $id. Current broker epoch is $cachedBrokerEpoch."
        info(stateBrokerEpochErrorMessage)
        throw new StaleBrokerEpochException(stateBrokerEpochErrorMessage)
      }
    }

    info(s"Shutting down broker $id")

    if (!controllerContext.liveOrShuttingDownBrokerIds.contains(id))
      throw new BrokerNotAvailableException(s"Broker id $id does not exist.")

    controllerContext.shuttingDownBrokerIds.add(id)
    debug(s"All shutting down brokers: ${controllerContext.shuttingDownBrokerIds.mkString(",")}")
    debug(s"Live brokers: ${controllerContext.liveBrokerIds.mkString(",")}")

    val partitionsToActOn = controllerContext.partitionsOnBroker(id).filter { partition =>
      controllerContext.partitionReplicaAssignment(partition).size > 1 &&
        controllerContext.partitionLeadershipInfo(partition).isDefined &&
        !topicDeletionManager.isTopicQueuedUpForDeletion(partition.topic)
    }
    val (partitionsLedByBroker, partitionsFollowedByBroker) = partitionsToActOn.partition { partition =>
      controllerContext.partitionLeadershipInfo(partition).get.leaderAndIsr.leader == id
    }
    // handle State Changes and select Leader according to the specified strategy
    // sendLeaderAndIsrRequest(controllerEpoch, stateChangeLog)
    // sendUpdateMetadataRequests(controllerEpoch, stateChangeLog)
    // sendStopReplicaRequests(controllerEpoch, stateChangeLog)
    partitionStateMachine.handleStateChanges(partitionsLedByBroker.toSeq, OnlinePartition, Some(ControlledShutdownPartitionLeaderElectionStrategy))
    try {
      brokerRequestBatch.newBatch()
      partitionsFollowedByBroker.foreach { partition =>
        brokerRequestBatch.addStopReplicaRequestForBrokers(Seq(id), partition, deletePartition = false)
      }
      brokerRequestBatch.sendRequestsToBrokers(epoch)
    } catch {
      case e: IllegalStateException =>
        handleIllegalState(e)
    }
    // If the broker is a follower, updates the isr in ZK and notifies the current leader
    replicaStateMachine.handleStateChanges(partitionsFollowedByBroker.map(partition =>
      PartitionAndReplica(partition, id)).toSeq, OfflineReplica)
    trace(s"All leaders = ${controllerContext.partitionsLeadershipInfo.mkString(",")}")
    controllerContext.partitionLeadersOnBroker(id)
  }

  private def processUpdateMetadataResponseReceived(updateMetadataResponse: UpdateMetadataResponse, brokerId: Int): Unit = {
    if (!isActive) return

    if (updateMetadataResponse.error != Errors.NONE) {
      stateChangeLogger.error(s"Received error ${updateMetadataResponse.error} in UpdateMetadata " +
        s"response $updateMetadataResponse from broker $brokerId")
    }
  }

  private def processLeaderAndIsrResponseReceived(leaderAndIsrResponse: LeaderAndIsrResponse, brokerId: Int): Unit = {
    if (!isActive) return

    if (leaderAndIsrResponse.error != Errors.NONE) {
      stateChangeLogger.error(s"Received error ${leaderAndIsrResponse.error} in LeaderAndIsr " +
        s"response $leaderAndIsrResponse from broker $brokerId")
      return
    }

    val offlineReplicas = new ArrayBuffer[TopicPartition]()
    val onlineReplicas = new ArrayBuffer[TopicPartition]()

    leaderAndIsrResponse.partitionErrors(controllerContext.topicNames.asJava).forEach{ case (tp, error) =>
      if (error.code() == Errors.KAFKA_STORAGE_ERROR.code)
        offlineReplicas += tp
      else if (error.code() == Errors.NONE.code)
        onlineReplicas += tp
    }

    val previousOfflineReplicas = controllerContext.replicasOnOfflineDirs.getOrElse(brokerId, Set.empty[TopicPartition])
    val currentOfflineReplicas = mutable.Set() ++= previousOfflineReplicas --= onlineReplicas ++= offlineReplicas
    controllerContext.replicasOnOfflineDirs.put(brokerId, currentOfflineReplicas)
    val newOfflineReplicas = currentOfflineReplicas.diff(previousOfflineReplicas)

    if (newOfflineReplicas.nonEmpty) {
      stateChangeLogger.info(s"Mark replicas ${newOfflineReplicas.mkString(",")} on broker $brokerId as offline")
      onReplicasBecomeOffline(newOfflineReplicas.map(PartitionAndReplica(_, brokerId)))
    }
  }

  /**
   * KafkaController should do "processTopicDeletionStopReplicaResponseReceived" after "single brokers done topic deleted"
   * @param replicaId
   * @param requestError
   * @param partitionErrors
   */
  private def processTopicDeletionStopReplicaResponseReceived(replicaId: Int,
                                                              requestError: Errors,
                                                              partitionErrors: Map[TopicPartition, Errors]): Unit = {
    // 1. if not controller, return directly
    if (!isActive) return
    debug(s"Delete topic callback invoked on StopReplica response received from broker $replicaId: " +
      s"request error = $requestError, partition errors = $partitionErrors")

    // 2. Get partitions with errors
    val partitionsInError = if (requestError != Errors.NONE)
      partitionErrors.keySet
    else
      partitionErrors.filter { case (_, error) => error != Errors.NONE }.keySet

    // 3. Get replicas with errors of above corresponding partition
    val replicasInError = partitionsInError.map(PartitionAndReplica(_, replicaId))

    // step 4 & 5 will do "resumeDeletions() | restarting the topic deletion action"
    // 4. Replicas deletion failure handling
    // move all the failed replicas to ReplicaDeletionIneligible
    topicDeletionManager.failReplicaDeletion(replicasInError)

    // 5. Replicas deletion successful handling
    if (replicasInError.size != partitionErrors.size) {
      // some replicas could have been successfully deleted
      val deletedReplicas = partitionErrors.keySet.diff(partitionsInError)
      // 收到成功删除后处理
      topicDeletionManager.completeReplicaDeletion(deletedReplicas.map(PartitionAndReplica(_, replicaId)))
    }
  }

  /**
   * kafkaController.startUp会放入StartUp事件，此为处理的方法
   * "处理”-/controller节点
   */
  private def processStartup(): Unit = {
    // 1. register "/controller" watch and check is exist?
    // 注册 controllerChangeHandler -> 时刻准备抢 controller 位置
    zkClient.registerZNodeChangeHandlerAndCheckExistence(controllerChangeHandler)
    // 2. handel elect
    // 不管怎么样，先看一眼，试一把
    // case1: "/controller" exist -> get controller information from zk node
    // case2: "/controller" not exist -> do controller elect
    elect()
  }

  private def updateMetrics(): Unit = {
    if (isActive) {
      offlinePartitionCount = controllerContext.offlinePartitionCount
      preferredReplicaImbalanceCount = controllerContext.preferredReplicaImbalanceCount
      globalTopicCount = controllerContext.allTopics.size
      globalPartitionCount = controllerContext.partitionWithLeadersCount
      topicsToDeleteCount = controllerContext.topicsToBeDeleted.size
      replicasToDeleteCount = controllerContext.topicsToBeDeleted.map { topic =>
        // For each enqueued topic, count the number of replicas that are not yet deleted
        controllerContext.replicasForTopic(topic).count { replica =>
          controllerContext.replicaState(replica) != ReplicaDeletionSuccessful
        }
      }.sum
      ineligibleTopicsToDeleteCount = controllerContext.topicsIneligibleForDeletion.size
      ineligibleReplicasToDeleteCount = controllerContext.topicsToBeDeleted.map { topic =>
        // For each enqueued topic, count the number of replicas that are ineligible
        controllerContext.replicasForTopic(topic).count { replica =>
          controllerContext.replicaState(replica) == ReplicaDeletionIneligible
        }
      }.sum
      activeBrokerCount = controllerContext.liveOrShuttingDownBrokerIds.size
    } else {
      offlinePartitionCount = 0
      preferredReplicaImbalanceCount = 0
      globalTopicCount = 0
      globalPartitionCount = 0
      topicsToDeleteCount = 0
      replicasToDeleteCount = 0
      ineligibleTopicsToDeleteCount = 0
      ineligibleReplicasToDeleteCount = 0
      activeBrokerCount = 0
    }
  }

  // visible for testing
  private[controller] def handleIllegalState(e: IllegalStateException): Nothing = {
    // Resign if the controller is in an illegal state
    error("Forcing the controller to resign")
    brokerRequestBatch.clear()
    triggerControllerMove()
    throw e
  }

  private def triggerControllerMove(): Unit = {
    activeControllerId = zkClient.getControllerId.getOrElse(-1)
    if (!isActive) {
      warn("Controller has already moved when trying to trigger controller movement")
      return
    }
    try {
      val expectedControllerEpochZkVersion = controllerContext.epochZkVersion
      activeControllerId = -1
      // controller卸任
      onControllerResignation()
      zkClient.deleteController(expectedControllerEpochZkVersion)
    } catch {
      case _: ControllerMovedException =>
        warn("Controller has already moved when trying to trigger controller movement")
    }
  }

  /**
   * Resign may be required
   */
  private def maybeResign(): Unit = {
    // 1. judge broker self Was it a leader before?
    val wasActiveBeforeChange = isActive
    // 2. register ZNodeChangeHandler
    zkClient.registerZNodeChangeHandlerAndCheckExistence(controllerChangeHandler)
    // 3. get the broker ID of current controller
    activeControllerId = zkClient.getControllerId.getOrElse(-1)
    // 4. if before is leader but Not anymore
    if (wasActiveBeforeChange && !isActive) {
      // 5. Perform an offload operation
      onControllerResignation()
    }
  }

  /**
   * do elect
   */
  private def elect(): Unit = {
    // 1. get the broker ID of current controller
    activeControllerId = zkClient.getControllerId.getOrElse(-1)

    /* 2. There is already a controller, return directly
     * We can get here during the initial startup and the handleDeleted ZK callback. Because of the potential race condition,
     * it's possible that the controller has already been elected when we get here. This check will prevent the following
     * createEphemeralPath method from getting into an infinite loop if this broker is already the controller.
     */
    if (activeControllerId != -1) {
      debug(s"Broker $activeControllerId has been elected as the controller, so stopping the election process.")
      return
    }

    try {
      // 3. try to create /controller
      // if create fail -> will throw ControllerMovedException, so Will not perform step 4
      // if create success -> will do step 4, Perform subsequent actions after election
      // 这步走完会创建/controller, 或者往外抛异常
      val (epoch, epochZkVersion) = zkClient.registerControllerAndIncrementControllerEpoch(config.brokerId)
      controllerContext.epoch = epoch
      controllerContext.epochZkVersion = epochZkVersion
      activeControllerId = config.brokerId

      info(s"${config.brokerId} successfully elected as the controller. Epoch incremented to ${controllerContext.epoch} " +
        s"and epoch zk version is now ${controllerContext.epochZkVersion}")

      // 4. leader elector on electing the current broker as the new controller
      // 新选举了controller，会调用处理 Controller 切换的方法
      onControllerFailover()
    } catch {
      case e: ControllerMovedException =>
        // ** this Exception means other broker become the Controller, so Resign may be required
        // 这个异常表明抢Controller没成功，其他broker成为了Controller， 此时需要执行maybeResign
        // 为啥是maybe呢？
        // 因为只有之前是Controller，才需要Resign来清空之前作为Controller维护的状态，信息等
        // 如果之前就不是Controller，那之前就没维护过什么信息，失败就失败，啥也不用干
        maybeResign()
        if (activeControllerId != -1)
          debug(s"Broker $activeControllerId was elected as controller instead of broker ${config.brokerId}", e)
        else
          warn("A controller has been elected but just resigned, this will result in another round of election", e)
      case t: Throwable =>
        error(s"Error while electing or becoming controller on broker ${config.brokerId}. " +
          s"Trigger controller movement immediately", t)
        triggerControllerMove()
    }
  }

  /**
   * Partitions the provided map of brokers and epochs into 2 new maps:
   *  - The first map contains only those brokers whose features were found to be compatible with
   *    the existing finalized features.
   *  - The second map contains only those brokers whose features were found to be incompatible with
   *    the existing finalized features.
   *
   * @param brokersAndEpochs   the map to be partitioned
   * @return                   two maps: first contains compatible brokers and second contains
   *                           incompatible brokers as explained above
   */
  private def partitionOnFeatureCompatibility(brokersAndEpochs: Map[Broker, Long]): (Map[Broker, Long], Map[Broker, Long]) = {
    // There can not be any feature incompatibilities when the feature versioning system is disabled
    // or when the finalized feature cache is empty. Otherwise, we check if the non-empty contents
    //  of the cache are compatible with the supported features of each broker.
    brokersAndEpochs.partition {
      case (broker, _) =>
        !config.isFeatureVersioningSupported ||
        !featureCache.getFeatureOption.exists(
          latestFinalizedFeatures =>
            BrokerFeatures.hasIncompatibleFeatures(broker.features,
              latestFinalizedFeatures.finalizedFeatures().asScala.
                map(kv => (kv._1, kv._2.toShort)).toMap))
    }
  }

  /**
   * process Broker state Change event: /brokers/ids
   * broker上线会往此节点下注册信息，
   * 只有Controller能处理
   * 其他broker虽然会监听此节点，但不做任何处理 ? 这个说法可能是不对的，因为这个对应着“brokerChangeHandler”，而这一Handler的注册，仅在elect()成功后会register
   */
  private def processBrokerChange(): Unit = {
    // 1. Returns true if this broker is the current controller
    if (!isActive) return
    // 2. 列表A: get cluster brokers from zookeeper -> [curBrokerIds]
    val curBrokerAndEpochs = zkClient.getAllBrokerAndEpochsInCluster
    val curBrokerIdAndEpochs = curBrokerAndEpochs map { case (broker, epoch) => (broker.id, epoch) }
    val curBrokerIds = curBrokerIdAndEpochs.keySet
    // 3. 列表B: get cluster brokers from controllerContext -> [liveOrShuttingDownBrokerIds]
    val liveOrShuttingDownBrokerIds = controllerContext.liveOrShuttingDownBrokerIds
    // 4. get newBrokerIds + deadBrokerIds + bouncedBrokerIds
    val newBrokerIds = curBrokerIds.diff(liveOrShuttingDownBrokerIds)
    val deadBrokerIds = liveOrShuttingDownBrokerIds.diff(curBrokerIds)
    // curBrokerIdAndEpochs中的版本号大于controllerContext.liveBrokerIdAndEpochs中的版本号, 对应了经历了重启或者状态更新的broker
    val bouncedBrokerIds = (curBrokerIds & liveOrShuttingDownBrokerIds)
      .filter(brokerId => curBrokerIdAndEpochs(brokerId) > controllerContext.liveBrokerIdAndEpochs(brokerId))
    // 5. Broker And Epochs
    val newBrokerAndEpochs = curBrokerAndEpochs.filter { case (broker, _) => newBrokerIds.contains(broker.id) }
    val bouncedBrokerAndEpochs = curBrokerAndEpochs.filter { case (broker, _) => bouncedBrokerIds.contains(broker.id) }
    // 6. 上面一通操作，获取下列4类broker的列表
    // use Ids sorted
    val newBrokerIdsSorted = newBrokerIds.toSeq.sorted
    val deadBrokerIdsSorted = deadBrokerIds.toSeq.sorted
    val liveBrokerIdsSorted = curBrokerIds.toSeq.sorted
    val bouncedBrokerIdsSorted = bouncedBrokerIds.toSeq.sorted
    info(s"Newly added brokers: ${newBrokerIdsSorted.mkString(",")}, " +
      s"deleted brokers: ${deadBrokerIdsSorted.mkString(",")}, " +
      s"bounced brokers: ${bouncedBrokerIdsSorted.mkString(",")}, " +
      s"all live brokers: ${liveBrokerIdsSorted.mkString(",")}")

    // 处理阶段1： 对应变更，导致的Controller与Broker之间的连接，及RequestSendThread所需的变更处理
    // 7. handle newBrokers, start RequestSendThread
    newBrokerAndEpochs.keySet.foreach(controllerChannelManager.addBroker)

    // 8. handle bouncedBrokers, remove RequestSendThread
    bouncedBrokerIds.foreach(controllerChannelManager.removeBroker)
    bouncedBrokerAndEpochs.keySet.foreach(controllerChannelManager.addBroker)

    // 9. handle deadBrokers, remove RequestSendThread
    deadBrokerIds.foreach(controllerChannelManager.removeBroker)

    // 处理阶段2： 实质broker上下线所需的动作
    // 10. handle newBrokers
    if (newBrokerIds.nonEmpty) {
      val (newCompatibleBrokerAndEpochs, newIncompatibleBrokerAndEpochs) =
        partitionOnFeatureCompatibility(newBrokerAndEpochs)
      if (newIncompatibleBrokerAndEpochs.nonEmpty) {
        warn("Ignoring registration of new brokers due to incompatibilities with finalized features: " +
          newIncompatibleBrokerAndEpochs.map { case (broker, _) => broker.id }.toSeq.sorted.mkString(","))
      }
      controllerContext.addLiveBrokers(newCompatibleBrokerAndEpochs)
      // 最核心的动作
      onBrokerStartup(newBrokerIdsSorted)
    }
    // 11. handle bouncedBrokers
    if (bouncedBrokerIds.nonEmpty) {
      controllerContext.removeLiveBrokers(bouncedBrokerIds)
      onBrokerFailure(bouncedBrokerIdsSorted)
      val (bouncedCompatibleBrokerAndEpochs, bouncedIncompatibleBrokerAndEpochs) =
        partitionOnFeatureCompatibility(bouncedBrokerAndEpochs)
      if (bouncedIncompatibleBrokerAndEpochs.nonEmpty) {
        warn("Ignoring registration of bounced brokers due to incompatibilities with finalized features: " +
          bouncedIncompatibleBrokerAndEpochs.map { case (broker, _) => broker.id }.toSeq.sorted.mkString(","))
      }
      controllerContext.addLiveBrokers(bouncedCompatibleBrokerAndEpochs)
      onBrokerStartup(bouncedBrokerIdsSorted)
    }
    // 12. handle deadBrokers
    if (deadBrokerIds.nonEmpty) {
      controllerContext.removeLiveBrokers(deadBrokerIds)
      onBrokerFailure(deadBrokerIdsSorted)
    }

    if (newBrokerIds.nonEmpty || deadBrokerIds.nonEmpty || bouncedBrokerIds.nonEmpty) {
      info(s"Updated broker epochs cache: ${controllerContext.liveBrokerIdAndEpochs}")
    }
  }

  /**
   * process Broker information/data Change event: /brokers/ids/${id}
   */
  private def processBrokerModification(brokerId: Int): Unit = {
    if (!isActive) return
    // 1. get data from zk
    val newMetadataOpt = zkClient.getBroker(brokerId)
    // 2. get data from controller
    val oldMetadataOpt = controllerContext.liveOrShuttingDownBroker(brokerId)

    // 3. Compare metadata and maybe update
    if (newMetadataOpt.nonEmpty && oldMetadataOpt.nonEmpty) {
      val oldMetadata = oldMetadataOpt.get
      val newMetadata = newMetadataOpt.get
      if (newMetadata.endPoints != oldMetadata.endPoints || !oldMetadata.features.equals(newMetadata.features)) {
        info(s"Updated broker metadata: $oldMetadata -> $newMetadata")
        // 3.1 controllerContext update
        controllerContext.updateBrokerMetadata(oldMetadata, newMetadata)
        // 3.2 send UpdateMetadata Request to all brokers in cluster
        onBrokerUpdate(brokerId)
      }
    }
  }

  private def processTopicChange(): Unit = {
    if (!isActive) return
    // 1. get from zk: /brokers/topics
    val topics = zkClient.getAllTopicsInCluster(true)
    // 2. find new topics
    val newTopics = topics -- controllerContext.allTopics
    // 3. find to be deleted topics
    val deletedTopics = controllerContext.allTopics.diff(topics)

    // 4. update topics data keep in controllerContext
    controllerContext.setAllTopics(topics)

    // 5. register Partition attach hook
    registerPartitionModificationsHandlers(newTopics.toSeq)

    // 6. get "newTopics" Partition Replica from zk "/brokers/topics/{newTopicName}"
    val addedPartitionReplicaAssignment = zkClient.getReplicaAssignmentAndTopicIdForTopics(newTopics)

    // 7. remove deleted Topics
    deletedTopics.foreach(controllerContext.removeTopic)

    // 8. update new topic Partition Replica
    processTopicIds(addedPartitionReplicaAssignment)

    addedPartitionReplicaAssignment.foreach { case TopicIdReplicaAssignment(_, _, newAssignments) =>
      newAssignments.foreach { case (topicAndPartition, newReplicaAssignment) =>
        controllerContext.updatePartitionFullReplicaAssignment(topicAndPartition, newReplicaAssignment)
      }
    }
    // * important log
    info(s"New topics: [$newTopics], deleted topics: [$deletedTopics], new partition replica assignment " +
      s"[$addedPartitionReplicaAssignment]")

    // 9. if have "new added Partition", to do onNewPartitionCreation
    if (addedPartitionReplicaAssignment.nonEmpty) {
      val partitionAssignments = addedPartitionReplicaAssignment
        .map { case TopicIdReplicaAssignment(_, _, partitionsReplicas) => partitionsReplicas.keySet }
        .reduce((s1, s2) => s1.union(s2))
      // the entry of "Partition status flow"
      onNewPartitionCreation(partitionAssignments)
    }
  }

  private def processTopicIds(topicIdAssignments: Set[TopicIdReplicaAssignment]): Unit = {
    // Create topic IDs for topics missing them if we are using topic IDs
    // Otherwise, maintain what we have in the topicZNode
    val updatedTopicIdAssignments = if (config.usesTopicId) {
      val (withTopicIds, withoutTopicIds) = topicIdAssignments.partition(_.topicId.isDefined)
      withTopicIds ++ zkClient.setTopicIds(withoutTopicIds, controllerContext.epochZkVersion)
    } else {
      topicIdAssignments
    }

    // Add topic IDs to controller context
    // If we don't have IBP 2.8, but are running 2.8 code, put any topic IDs from the ZNode in controller context
    // This is to avoid losing topic IDs during operations like partition reassignments while the cluster is in a mixed state
    updatedTopicIdAssignments.foreach { topicIdAssignment =>
      topicIdAssignment.topicId.foreach { topicId =>
        controllerContext.addTopicId(topicIdAssignment.topic, topicId)
    }
    }
  }

  private def processLogDirEventNotification(): Unit = {
    if (!isActive) return
    val sequenceNumbers = zkClient.getAllLogDirEventNotifications
    try {
      val brokerIds = zkClient.getBrokerIdsFromLogDirEvents(sequenceNumbers)
      onBrokerLogDirFailure(brokerIds)
    } finally {
      // delete processed children
      zkClient.deleteLogDirEventNotifications(sequenceNumbers, controllerContext.epochZkVersion)
    }
  }

  private def processPartitionModifications(topic: String): Unit = {
    def restorePartitionReplicaAssignment(
      topic: String,
      newPartitionReplicaAssignment: Map[TopicPartition, ReplicaAssignment]
    ): Unit = {
      info("Restoring the partition replica assignment for topic %s".format(topic))

      val existingPartitions = zkClient.getChildren(TopicPartitionsZNode.path(topic))
      val existingPartitionReplicaAssignment = newPartitionReplicaAssignment
        .filter(p => existingPartitions.contains(p._1.partition.toString))
        .map { case (tp, _) =>
          tp -> controllerContext.partitionFullReplicaAssignment(tp)
      }.toMap

      zkClient.setTopicAssignment(topic,
        controllerContext.topicIds.get(topic),
        existingPartitionReplicaAssignment,
        controllerContext.epochZkVersion)
    }

    if (!isActive) return
    val partitionReplicaAssignment = zkClient.getFullReplicaAssignmentForTopics(immutable.Set(topic))
    val partitionsToBeAdded = partitionReplicaAssignment.filter { case (topicPartition, _) =>
      controllerContext.partitionReplicaAssignment(topicPartition).isEmpty
    }

    if (topicDeletionManager.isTopicQueuedUpForDeletion(topic)) {
      if (partitionsToBeAdded.nonEmpty) {
        warn("Skipping adding partitions %s for topic %s since it is currently being deleted"
          .format(partitionsToBeAdded.map(_._1.partition).mkString(","), topic))

        restorePartitionReplicaAssignment(topic, partitionReplicaAssignment)
      } else {
        // This can happen if existing partition replica assignment are restored to prevent increasing partition count during topic deletion
        info("Ignoring partition change during topic deletion as no new partitions are added")
      }
    } else if (partitionsToBeAdded.nonEmpty) {
      info(s"New partitions to be added $partitionsToBeAdded")
      partitionsToBeAdded.forKeyValue { (topicPartition, assignedReplicas) =>
        controllerContext.updatePartitionFullReplicaAssignment(topicPartition, assignedReplicas)
      }
      onNewPartitionCreation(partitionsToBeAdded.keySet)
    }
  }
  // controller监听到/admin/delete_topics/{TopicName}节点变化
  private def processTopicDeletion(): Unit = {
    // 1. if not Controller, return directly
    if (!isActive) return
    // 2. get topics to delete | ${AdminZNode.path}/delete_topics
    var topicsToBeDeleted = zkClient.getTopicDeletions.toSet
    debug(s"Delete topics listener fired for topics ${topicsToBeDeleted.mkString(",")} to be deleted")
    // 3. Compare the list of topics to be deleted in zk and controllerContext, and determine topicsToBeDeleted
    val nonExistentTopics = topicsToBeDeleted -- controllerContext.allTopics
    if (nonExistentTopics.nonEmpty) {
      warn(s"Ignoring request to delete non-existing topics ${nonExistentTopics.mkString(",")}")
      zkClient.deleteTopicDeletions(nonExistentTopics.toSeq, controllerContext.epochZkVersion)
    }
    topicsToBeDeleted --= nonExistentTopics
    // 4. delete topics
    if (config.deleteTopicEnable) {
      if (topicsToBeDeleted.nonEmpty) {
        // important log
        info(s"Starting topic deletion for topics ${topicsToBeDeleted.mkString(",")}")

        // 4.1 mark topic ineligible for deletion if other state changes are in progress
        // if "topic reassignment in progress", can not delete immediatily
        // 这里停止删除是“暂时不删除，后续等可以删除了再删除”，还是就是不删除了？
        // TopicDeletionManager里面的注释很好的解答了
        // ----------------------------------------------------------
//        * 3. The controller's ControllerEventThread handles topic deletion. A topic will be ineligible
//        *    for deletion in the following scenarios -
//          *   3.1 broker hosting one of the replicas for that topic goes down
//          *   3.2 partition reassignment for partitions of that topic is in progress
//        * 4. Topic deletion is resumed when -
//          *    4.1 broker hosting one of the replicas for that topic is started
//          *    4.2 partition reassignment for partitions of that topic complete
        // ----------------------------------------------------------

        topicsToBeDeleted.foreach { topic =>
          val partitionReassignmentInProgress =
            controllerContext.partitionsBeingReassigned.map(_.topic).contains(topic)
          if (partitionReassignmentInProgress)
            topicDeletionManager.markTopicIneligibleForDeletion(Set(topic),
              reason = "topic reassignment in progress")
        }
        // 4.2 add topic to deletion list： 放入独立线程的删除队列去删除
        topicDeletionManager.enqueueTopicsForDeletion(topicsToBeDeleted)
      }
    } else {
      // 5. If delete topic is disabled remove entries under zookeeper path : /admin/delete_topics
      info(s"Removing $topicsToBeDeleted since delete topic is disabled")
      zkClient.deleteTopicDeletions(topicsToBeDeleted.toSeq, controllerContext.epochZkVersion)
    }
  }

  private def processZkPartitionReassignment(): Set[TopicPartition] = {
    // We need to register the watcher if the path doesn't exist in order to detect future
    // reassignments and we get the `path exists` check for free
    if (isActive && zkClient.registerZNodeChangeHandlerAndCheckExistence(partitionReassignmentHandler)) {
      val reassignmentResults = mutable.Map.empty[TopicPartition, ApiError]
      val partitionsToReassign = mutable.Map.empty[TopicPartition, ReplicaAssignment]

      zkClient.getPartitionReassignment.forKeyValue { (tp, targetReplicas) =>
        maybeBuildReassignment(tp, Some(targetReplicas)) match {
          case Some(context) => partitionsToReassign.put(tp, context)
          case None => reassignmentResults.put(tp, new ApiError(Errors.NO_REASSIGNMENT_IN_PROGRESS))
        }
      }

      reassignmentResults ++= maybeTriggerPartitionReassignment(partitionsToReassign)
      val (partitionsReassigned, partitionsFailed) = reassignmentResults.partition(_._2.error == Errors.NONE)
      if (partitionsFailed.nonEmpty) {
        warn(s"Failed reassignment through zk with the following errors: $partitionsFailed")
        maybeRemoveFromZkReassignment((tp, _) => partitionsFailed.contains(tp))
      }
      partitionsReassigned.keySet
    } else {
      Set.empty
    }
  }

  /**
   * Process a partition reassignment from the AlterPartitionReassignment API. If there is an
   * existing reassignment through zookeeper for any of the requested partitions, they will be
   * cancelled prior to beginning the new reassignment. Any zk-based reassignment for partitions
   * which are NOT included in this call will not be affected.
   *
   * @param reassignments Map of reassignments passed through the AlterReassignments API. A null value
   *                      means that we should cancel an in-progress reassignment.
   * @param callback Callback to send AlterReassignments response
   */
  private def processApiPartitionReassignment(reassignments: Map[TopicPartition, Option[Seq[Int]]],
                                              callback: AlterReassignmentsCallback): Unit = {
    if (!isActive) {
      callback(Right(new ApiError(Errors.NOT_CONTROLLER)))
    } else {
      val reassignmentResults = mutable.Map.empty[TopicPartition, ApiError]
      val partitionsToReassign = mutable.Map.empty[TopicPartition, ReplicaAssignment]

      reassignments.forKeyValue { (tp, targetReplicas) =>
        val maybeApiError = targetReplicas.flatMap(validateReplicas(tp, _))
        maybeApiError match {
          case None =>
            maybeBuildReassignment(tp, targetReplicas) match {
              case Some(context) => partitionsToReassign.put(tp, context)
              case None => reassignmentResults.put(tp, new ApiError(Errors.NO_REASSIGNMENT_IN_PROGRESS))
            }
          case Some(err) =>
            reassignmentResults.put(tp, err)
        }
      }

      // The latest reassignment (whether by API or through zk) always takes precedence,
      // so remove from active zk reassignment (if one exists)
      maybeRemoveFromZkReassignment((tp, _) => partitionsToReassign.contains(tp))

      reassignmentResults ++= maybeTriggerPartitionReassignment(partitionsToReassign)
      callback(Left(reassignmentResults))
    }
  }

  private def validateReplicas(topicPartition: TopicPartition, replicas: Seq[Int]): Option[ApiError] = {
    val replicaSet = replicas.toSet
    if (replicas.isEmpty)
      Some(new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT,
          s"Empty replica list specified in partition reassignment."))
    else if (replicas.size != replicaSet.size) {
      Some(new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT,
          s"Duplicate replica ids in partition reassignment replica list: $replicas"))
    } else if (replicas.exists(_ < 0))
      Some(new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT,
          s"Invalid broker id in replica list: $replicas"))
    else {
      // Ensure that any new replicas are among the live brokers
      val currentAssignment = controllerContext.partitionFullReplicaAssignment(topicPartition)
      val newAssignment = currentAssignment.reassignTo(replicas)
      val areNewReplicasAlive = newAssignment.addingReplicas.toSet.subsetOf(controllerContext.liveBrokerIds)
      if (!areNewReplicasAlive)
        Some(new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT,
          s"Replica assignment has brokers that are not alive. Replica list: " +
            s"${newAssignment.addingReplicas}, live broker list: ${controllerContext.liveBrokerIds}"))
      else None
    }
  }

  private def maybeBuildReassignment(topicPartition: TopicPartition,
                                     targetReplicasOpt: Option[Seq[Int]]): Option[ReplicaAssignment] = {
    val replicaAssignment = controllerContext.partitionFullReplicaAssignment(topicPartition)
    if (replicaAssignment.isBeingReassigned) {
      val targetReplicas = targetReplicasOpt.getOrElse(replicaAssignment.originReplicas)
      Some(replicaAssignment.reassignTo(targetReplicas))
    } else {
      targetReplicasOpt.map { targetReplicas =>
        replicaAssignment.reassignTo(targetReplicas)
      }
    }
  }

  private def processPartitionReassignmentIsrChange(topicPartition: TopicPartition): Unit = {
    if (!isActive) return

    if (controllerContext.partitionsBeingReassigned.contains(topicPartition)) {
      maybeCompleteReassignment(topicPartition)
    }
  }

  private def maybeCompleteReassignment(topicPartition: TopicPartition): Unit = {
    val reassignment = controllerContext.partitionFullReplicaAssignment(topicPartition)
    if (isReassignmentComplete(topicPartition, reassignment)) {
      // resume the partition reassignment process
      info(s"Target replicas ${reassignment.targetReplicas} have all caught up with the leader for " +
        s"reassigning partition $topicPartition")
      onPartitionReassignment(topicPartition, reassignment)
    }
  }

  private def processListPartitionReassignments(partitionsOpt: Option[Set[TopicPartition]], callback: ListReassignmentsCallback): Unit = {
    if (!isActive) {
      callback(Right(new ApiError(Errors.NOT_CONTROLLER)))
    } else {
      val results: mutable.Map[TopicPartition, ReplicaAssignment] = mutable.Map.empty
      val partitionsToList = partitionsOpt match {
        case Some(partitions) => partitions
        case None => controllerContext.partitionsBeingReassigned
      }

      partitionsToList.foreach { tp =>
        val assignment = controllerContext.partitionFullReplicaAssignment(tp)
        if (assignment.isBeingReassigned) {
          results += tp -> assignment
        }
      }

      callback(Left(results))
    }
  }

  /**
   * Returns the new finalized version for the feature, if there are no feature
   * incompatibilities seen with all known brokers for the provided feature update.
   * Otherwise returns an ApiError object containing Errors.INVALID_REQUEST.
   *
   * @param update   the feature update to be processed (this can not be meant to delete the feature)
   *
   * @return         the new finalized version or error, as described above.
   */
  private def newFinalizedVersionOrIncompatibilityError(update: UpdateFeaturesRequest.FeatureUpdateItem):
      Either[Short, ApiError] = {
    if (update.isDeleteRequest) {
      throw new IllegalArgumentException(s"Provided feature update can not be meant to delete the feature: $update")
    }

    val supportedVersionRange = brokerFeatures.supportedFeatures.get(update.feature)
    if (supportedVersionRange == null) {
      Right(new ApiError(Errors.INVALID_REQUEST,
                         "Could not apply finalized feature update because the provided feature" +
                         " is not supported."))
    } else {
      val newVersion = update.versionLevel()
      if (supportedVersionRange.isIncompatibleWith(newVersion)) {
        Right(new ApiError(Errors.INVALID_REQUEST,
          "Could not apply finalized feature update because the provided" +
          s" versionLevel:${update.versionLevel} is lower than the" +
          s" supported minVersion:${supportedVersionRange.min}."))
      } else {
        val newFinalizedFeature = Utils.mkMap(Utils.mkEntry(update.feature, newVersion)).asScala.toMap
        val numIncompatibleBrokers = controllerContext.liveOrShuttingDownBrokers.count(broker => {
          BrokerFeatures.hasIncompatibleFeatures(broker.features, newFinalizedFeature)
        })
        if (numIncompatibleBrokers == 0) {
          Left(newVersion)
        } else {
          Right(new ApiError(Errors.INVALID_REQUEST,
                             "Could not apply finalized feature update because" +
                             " brokers were found to have incompatible versions for the feature."))
        }
      }
    }
  }

  /**
   * Validates a feature update on an existing finalized version.
   * If the validation succeeds, then, the return value contains:
   * 1. the new finalized version for the feature, if the feature update was not meant to delete the feature.
   * 2. Option.empty, if the feature update was meant to delete the feature.
   *
   * If the validation fails, then returned value contains a suitable ApiError.
   *
   * @param update             the feature update to be processed.
   * @param existingVersion    the existing finalized version which can be empty when no
   *                           finalized version exists for the associated feature
   *
   * @return                   the new finalized version to be updated into ZK or error
   *                           as described above.
   */
  private def validateFeatureUpdate(update: UpdateFeaturesRequest.FeatureUpdateItem,
                                    existingVersion: Option[Short]): Either[Option[Short], ApiError] = {
    def newVersionRangeOrError(update: UpdateFeaturesRequest.FeatureUpdateItem): Either[Option[Short], ApiError] = {
      newFinalizedVersionOrIncompatibilityError(update)
        .fold(versionRange => Left(Some(versionRange)), error => Right(error))
    }

    if (update.feature.isEmpty) {
      // Check that the feature name is not empty.
      Right(new ApiError(Errors.INVALID_REQUEST, "Feature name can not be empty."))
    } else if (update.upgradeType.equals(UpgradeType.UNKNOWN)) {
      Right(new ApiError(Errors.INVALID_REQUEST, "Received unknown upgrade type."))
    } else {

      // We handle deletion requests separately from non-deletion requests.
      if (update.isDeleteRequest) {
        if (existingVersion.isEmpty) {
          // Disallow deletion of a non-existing finalized feature.
          Right(new ApiError(Errors.INVALID_REQUEST,
                             "Can not delete non-existing finalized feature."))
        } else {
          Left(Option.empty)
        }
      } else if (update.versionLevel() < 1) {
        // Disallow deletion of a finalized feature without SAFE downgrade type.
        Right(new ApiError(Errors.INVALID_REQUEST,
                           s"Can not provide versionLevel: ${update.versionLevel} less" +
                           s" than 1 without setting the SAFE downgradeType in the request."))
      } else {
        existingVersion.map(existing =>
          if (update.versionLevel == existing) {
            // Disallow a case where target versionLevel matches existing versionLevel.
            Right(new ApiError(Errors.INVALID_REQUEST,
                               s"Can not ${if (update.upgradeType.equals(UpgradeType.SAFE_DOWNGRADE)) "downgrade" else "upgrade"}" +
                               s" a finalized feature from existing versionLevel:$existing" +
                               " to the same value."))
          } else if (update.versionLevel < existing && !update.upgradeType.equals(UpgradeType.SAFE_DOWNGRADE)) {
            // Disallow downgrade of a finalized feature without the downgradeType set.
            Right(new ApiError(Errors.INVALID_REQUEST,
                               s"Can not downgrade finalized feature from existing" +
                               s" versionLevel:$existing to provided" +
                               s" versionLevel:${update.versionLevel} without setting the" +
                               " downgradeType to SAFE in the request."))
          } else if (!update.upgradeType.equals(UpgradeType.UPGRADE) && update.versionLevel > existing) {
            // Disallow a request that sets downgradeType without specifying a
            // versionLevel that's lower than the existing versionLevel.
            Right(new ApiError(Errors.INVALID_REQUEST,
                               s"When the downgradeType is set to SAFE in the request, the provided" +
                               s" versionLevel:${update.versionLevel} can not be greater than" +
                               s" existing versionLevel:$existing."))
          } else {
            newVersionRangeOrError(update)
          }
        ).getOrElse(newVersionRangeOrError(update))
      }
    }
  }

  private def processFeatureUpdates(request: UpdateFeaturesRequest,
                                    callback: UpdateFeaturesCallback): Unit = {
    if (isActive) {
      processFeatureUpdatesWithActiveController(request, callback)
    } else {
      callback(Left(new ApiError(Errors.NOT_CONTROLLER)))
    }
  }

  private def processFeatureUpdatesWithActiveController(request: UpdateFeaturesRequest,
                                                        callback: UpdateFeaturesCallback): Unit = {
    val updates = request.featureUpdates
    val existingFeatures = featureCache.getFeatureOption
      .map(featuresAndEpoch => featuresAndEpoch.finalizedFeatures().asScala.map(kv => (kv._1, kv._2.toShort)).toMap)
    .getOrElse(Map[String, Short]())
    // A map with key being feature name and value being finalized version.
    // This contains the target features to be eventually written to FeatureZNode.
    val targetFeatures = scala.collection.mutable.Map[String, Short]() ++ existingFeatures
    // A map with key being feature name and value being error encountered when the FeatureUpdate
    // was applied.
    val errors = scala.collection.mutable.Map[String, ApiError]()

    // Below we process each FeatureUpdate using the following logic:
    //  - If a FeatureUpdate is found to be valid, then:
    //    - The corresponding entry in errors map would be updated to contain Errors.NONE.
    //    - If the FeatureUpdate is an add or update request, then the targetFeatures map is updated
    //      to contain the new finalized version for the feature.
    //    - Otherwise if the FeatureUpdate is a delete request, then the feature is removed from the
    //      targetFeatures map.
    //  - Otherwise if a FeatureUpdate is found to be invalid, then:
    //    - The corresponding entry in errors map would be updated with the appropriate ApiError.
    //    - The entry in targetFeatures map is left untouched.
    updates.asScala.iterator.foreach { update =>
      validateFeatureUpdate(update, existingFeatures.get(update.feature())) match {
        case Left(newVersionRangeOrNone) =>
          newVersionRangeOrNone match {
            case Some(newVersionRange) => targetFeatures += (update.feature() -> newVersionRange)
            case None => targetFeatures -= update.feature()
          }
          errors += (update.feature() -> new ApiError(Errors.NONE))
        case Right(featureUpdateFailureReason) =>
          errors += (update.feature() -> featureUpdateFailureReason)
      }
    }

    // If the existing and target features are the same, then, we skip the update to the
    // FeatureZNode as no changes to the node are required. Otherwise, we replace the contents
    // of the FeatureZNode with the new features. This may result in partial or full modification
    // of the existing finalized features in ZK.
    try {
      if (!existingFeatures.equals(targetFeatures)) {
        val newNode = FeatureZNode(config.interBrokerProtocolVersion, FeatureZNodeStatus.Enabled, targetFeatures)
        val newVersion = updateFeatureZNode(newNode)
        featureCache.waitUntilFeatureEpochOrThrow(newVersion, request.data().timeoutMs())
      }
    } catch {
      // For all features that correspond to valid FeatureUpdate (i.e. error is Errors.NONE),
      // we set the error as Errors.FEATURE_UPDATE_FAILED since the FeatureZNode update has failed
      // for these. For the rest, the existing error is left untouched.
      case e: Exception =>
        warn(s"Processing of feature updates: $request failed due to error: $e")
        errors.foreach { case (feature, apiError) =>
          if (apiError.error() == Errors.NONE) {
            errors(feature) = new ApiError(Errors.FEATURE_UPDATE_FAILED)
          }
        }
    } finally {
      callback(Right(errors))
    }
  }

  private def processIsrChangeNotification(): Unit = {
    def processUpdateNotifications(partitions: Seq[TopicPartition]): Unit = {
      val liveBrokers: Seq[Int] = controllerContext.liveOrShuttingDownBrokerIds.toSeq
      debug(s"Sending MetadataRequest to Brokers: $liveBrokers for TopicPartitions: $partitions")
      sendUpdateMetadataRequest(liveBrokers, partitions.toSet)
    }

    if (!isActive) return
    val sequenceNumbers = zkClient.getAllIsrChangeNotifications
    try {
      val partitions = zkClient.getPartitionsFromIsrChangeNotifications(sequenceNumbers)
      if (partitions.nonEmpty) {
        updateLeaderAndIsrCache(partitions)
        processUpdateNotifications(partitions)

        // During a partial upgrade, the controller may be on an IBP which assumes
        // ISR changes through the `AlterPartition` API while some brokers are on an older
        // IBP which assumes notification through Zookeeper. In this case, since the
        // controller will not have registered watches for reassigning partitions, we
        // can still rely on the batch ISR change notification path in order to
        // complete the reassignment.
        partitions.filter(controllerContext.partitionsBeingReassigned.contains).foreach { topicPartition =>
          maybeCompleteReassignment(topicPartition)
        }
      }
    } finally {
      // delete the notifications
      zkClient.deleteIsrChangeNotifications(sequenceNumbers, controllerContext.epochZkVersion)
    }
  }

  def electLeaders(
    partitions: Set[TopicPartition],
    electionType: ElectionType,
    callback: ElectLeadersCallback
  ): Unit = {
    eventManager.put(ReplicaLeaderElection(Some(partitions), electionType, AdminClientTriggered, callback))
  }

  def listPartitionReassignments(partitions: Option[Set[TopicPartition]],
                                 callback: ListReassignmentsCallback): Unit = {
    eventManager.put(ListPartitionReassignments(partitions, callback))
  }

  def updateFeatures(request: UpdateFeaturesRequest,
                     callback: UpdateFeaturesCallback): Unit = {
    eventManager.put(UpdateFeatures(request, callback))
  }

  def alterPartitionReassignments(partitions: Map[TopicPartition, Option[Seq[Int]]],
                                  callback: AlterReassignmentsCallback): Unit = {
    eventManager.put(ApiPartitionReassignment(partitions, callback))
  }

  private def processReplicaLeaderElection(
    partitionsFromAdminClientOpt: Option[Set[TopicPartition]],
    electionType: ElectionType,
    electionTrigger: ElectionTrigger,
    callback: ElectLeadersCallback
  ): Unit = {
    if (!isActive) {
      callback(partitionsFromAdminClientOpt.fold(Map.empty[TopicPartition, Either[ApiError, Int]]) { partitions =>
        partitions.iterator.map(partition => partition -> Left(new ApiError(Errors.NOT_CONTROLLER, null))).toMap
      })
    } else {
      // We need to register the watcher if the path doesn't exist in order to detect future preferred replica
      // leader elections and we get the `path exists` check for free
      if (electionTrigger == AdminClientTriggered || zkClient.registerZNodeChangeHandlerAndCheckExistence(preferredReplicaElectionHandler)) {
        val partitions = partitionsFromAdminClientOpt match {
          case Some(partitions) => partitions
          case None => zkClient.getPreferredReplicaElection
        }

        val allPartitions = controllerContext.allPartitions
        val (knownPartitions, unknownPartitions) = partitions.partition(tp => allPartitions.contains(tp))
        unknownPartitions.foreach { p =>
          info(s"Skipping replica leader election ($electionType) for partition $p by $electionTrigger since it doesn't exist.")
        }

        val (partitionsBeingDeleted, livePartitions) = knownPartitions.partition(partition =>
            topicDeletionManager.isTopicQueuedUpForDeletion(partition.topic))
        if (partitionsBeingDeleted.nonEmpty) {
          warn(s"Skipping replica leader election ($electionType) for partitions $partitionsBeingDeleted " +
            s"by $electionTrigger since the respective topics are being deleted")
        }

        // partition those that have a valid leader
        val (electablePartitions, alreadyValidLeader) = livePartitions.partition { partition =>
          electionType match {
            case ElectionType.PREFERRED =>
              val assignedReplicas = controllerContext.partitionReplicaAssignment(partition)
              val preferredReplica = assignedReplicas.head
              val currentLeader = controllerContext.partitionLeadershipInfo(partition).get.leaderAndIsr.leader
              currentLeader != preferredReplica

            case ElectionType.UNCLEAN =>
              val currentLeader = controllerContext.partitionLeadershipInfo(partition).get.leaderAndIsr.leader
              currentLeader == LeaderAndIsr.NoLeader || !controllerContext.liveBrokerIds.contains(currentLeader)
          }
        }

        val results = onReplicaElection(electablePartitions, electionType, electionTrigger).map {
          case (k, Left(ex)) =>
            if (ex.isInstanceOf[StateChangeFailedException]) {
              val error = if (electionType == ElectionType.PREFERRED) {
                Errors.PREFERRED_LEADER_NOT_AVAILABLE
              } else {
                Errors.ELIGIBLE_LEADERS_NOT_AVAILABLE
              }
              k -> Left(new ApiError(error, ex.getMessage))
            } else {
              k -> Left(ApiError.fromThrowable(ex))
            }
          case (k, Right(leaderAndIsr)) => k -> Right(leaderAndIsr.leader)
        } ++
        alreadyValidLeader.map(_ -> Left(new ApiError(Errors.ELECTION_NOT_NEEDED))) ++
        partitionsBeingDeleted.map(
          _ -> Left(new ApiError(Errors.INVALID_TOPIC_EXCEPTION, "The topic is being deleted"))
        ) ++
        unknownPartitions.map(
          _ -> Left(new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION, "The partition does not exist."))
        )

        debug(s"Waiting for any successful result for election type ($electionType) by $electionTrigger for partitions: $results")
        callback(results)
      }
    }
  }

  def alterPartitions(
    alterPartitionRequest: AlterPartitionRequestData,
    alterPartitionRequestVersion: Short,
    callback: AlterPartitionResponseData => Unit
  ): Unit = {
    eventManager.put(AlterPartitionReceived(
      alterPartitionRequest,
      alterPartitionRequestVersion,
      callback
    ))
  }

  private def processAlterPartition(
    alterPartitionRequest: AlterPartitionRequestData,
    alterPartitionRequestVersion: Short,
    callback: AlterPartitionResponseData => Unit
  ): Unit = {
    val partitionResponses = try {
      tryProcessAlterPartition(
        alterPartitionRequest,
        alterPartitionRequestVersion,
        callback
      )
    } catch {
      case e: Throwable =>
        error(s"Error when processing AlterPartition: $alterPartitionRequest", e)
        callback(new AlterPartitionResponseData().setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code))
        mutable.Map.empty
    }

    // After we have returned the result of the `AlterPartition` request, we should check whether
    // there are any reassignments which can be completed by a successful ISR expansion.
    partitionResponses.forKeyValue { (topicPartition, partitionResponse) =>
      if (controllerContext.partitionsBeingReassigned.contains(topicPartition)) {
        val isSuccessfulUpdate = partitionResponse.isRight
        if (isSuccessfulUpdate) {
          maybeCompleteReassignment(topicPartition)
        }
      }
    }
  }

  private def tryProcessAlterPartition(
    alterPartitionRequest: AlterPartitionRequestData,
    alterPartitionRequestVersion: Short,
    callback: AlterPartitionResponseData => Unit
  ): mutable.Map[TopicPartition, Either[Errors, LeaderAndIsr]] = {
    val useTopicsIds = alterPartitionRequestVersion > 1

    // Handle a few short-circuits
    if (!isActive) {
      callback(new AlterPartitionResponseData().setErrorCode(Errors.NOT_CONTROLLER.code))
      return mutable.Map.empty
    }

    val brokerId = alterPartitionRequest.brokerId
    val brokerEpoch = alterPartitionRequest.brokerEpoch
    val brokerEpochOpt = controllerContext.liveBrokerIdAndEpochs.get(brokerId)
    if (brokerEpochOpt.isEmpty) {
      info(s"Ignoring AlterPartition due to unknown broker $brokerId")
      callback(new AlterPartitionResponseData().setErrorCode(Errors.STALE_BROKER_EPOCH.code))
      return mutable.Map.empty
    }

    if (!brokerEpochOpt.contains(brokerEpoch)) {
      info(s"Ignoring AlterPartition due to stale broker epoch $brokerEpoch and local broker epoch $brokerEpochOpt for broker $brokerId")
      callback(new AlterPartitionResponseData().setErrorCode(Errors.STALE_BROKER_EPOCH.code))
      return mutable.Map.empty
    }

    val partitionsToAlter = new mutable.HashMap[TopicPartition, LeaderAndIsr]()
    val alterPartitionResponse = new AlterPartitionResponseData()

    alterPartitionRequest.topics.forEach { topicReq =>
      val topicNameOpt = if (useTopicsIds) {
        controllerContext.topicName(topicReq.topicId)
      } else {
        Some(topicReq.topicName)
      }

      topicNameOpt match {
        case None =>
          val topicResponse = new AlterPartitionResponseData.TopicData()
            .setTopicId(topicReq.topicId)
          alterPartitionResponse.topics.add(topicResponse)
          topicReq.partitions.forEach { partitionReq =>
            topicResponse.partitions.add(new AlterPartitionResponseData.PartitionData()
              .setPartitionIndex(partitionReq.partitionIndex)
              .setErrorCode(Errors.UNKNOWN_TOPIC_ID.code))
          }

        case Some(topicName) =>
          topicReq.partitions.forEach { partitionReq =>
            val isr = if (alterPartitionRequestVersion >= 3) {
              partitionReq.newIsrWithEpochs.asScala.toList.map(brokerState => brokerState.brokerId())
            } else {
              partitionReq.newIsr.asScala.toList.map(_.toInt)
            }
            partitionsToAlter.put(
              new TopicPartition(topicName, partitionReq.partitionIndex),
              LeaderAndIsr(
                alterPartitionRequest.brokerId,
                partitionReq.leaderEpoch,
                isr,
                LeaderRecoveryState.of(partitionReq.leaderRecoveryState),
                partitionReq.partitionEpoch
              )
            )
          }
      }
    }

    val partitionResponses = mutable.HashMap[TopicPartition, Either[Errors, LeaderAndIsr]]()
    // Determine which partitions we will accept the new ISR for
    val adjustedIsrs = partitionsToAlter.flatMap { case (tp, newLeaderAndIsr) =>
      controllerContext.partitionLeadershipInfo(tp) match {
        case Some(leaderIsrAndControllerEpoch) =>
          val currentLeaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr
          if (newLeaderAndIsr.partitionEpoch > currentLeaderAndIsr.partitionEpoch
              || newLeaderAndIsr.leaderEpoch > currentLeaderAndIsr.leaderEpoch) {
            // If the partition leader has a higher partition/leader epoch, then it is likely
            // that this node is no longer the active controller. We return NOT_CONTROLLER in
            // this case to give the leader an opportunity to find the new controller.
            partitionResponses(tp) = Left(Errors.NOT_CONTROLLER)
            None
          } else if (newLeaderAndIsr.leaderEpoch < currentLeaderAndIsr.leaderEpoch) {
            partitionResponses(tp) = Left(Errors.FENCED_LEADER_EPOCH)
            None
          } else if (newLeaderAndIsr.equalsAllowStalePartitionEpoch(currentLeaderAndIsr)) {
            // If a partition is already in the desired state, just return it
            // this check must be done before fencing based on partition epoch to maintain idempotency
            partitionResponses(tp) = Right(currentLeaderAndIsr)
            None
          } else if (newLeaderAndIsr.partitionEpoch < currentLeaderAndIsr.partitionEpoch) {
            partitionResponses(tp) = Left(Errors.INVALID_UPDATE_VERSION)
            None
          }  else if (newLeaderAndIsr.leaderRecoveryState == LeaderRecoveryState.RECOVERING && newLeaderAndIsr.isr.length > 1) {
            partitionResponses(tp) = Left(Errors.INVALID_REQUEST)
            info(
              s"Rejecting AlterPartition from node $brokerId for $tp because leader is recovering and ISR is greater than 1: " +
              s"$newLeaderAndIsr"
            )
            None
          } else if (currentLeaderAndIsr.leaderRecoveryState == LeaderRecoveryState.RECOVERED &&
            newLeaderAndIsr.leaderRecoveryState == LeaderRecoveryState.RECOVERING) {

            partitionResponses(tp) = Left(Errors.INVALID_REQUEST)
            info(
              s"Rejecting AlterPartition from node $brokerId for $tp because the leader recovery state cannot change from " +
              s"RECOVERED to RECOVERING: $newLeaderAndIsr"
            )
            None
          } else {
            // Pull out replicas being added to ISR and verify they are all online.
            // If a replica is not online, reject the update as specified in KIP-841.
            val ineligibleReplicas = newLeaderAndIsr.isr.toSet -- controllerContext.liveBrokerIds
            if (ineligibleReplicas.nonEmpty) {
              info(s"Rejecting AlterPartition request from node $brokerId for $tp because " +
                s"it specified ineligible replicas $ineligibleReplicas in the new ISR ${newLeaderAndIsr.isr}."
              )

              if (alterPartitionRequestVersion > 1) {
                partitionResponses(tp) = Left(Errors.INELIGIBLE_REPLICA)
              } else {
                partitionResponses(tp) = Left(Errors.OPERATION_NOT_ATTEMPTED)
              }
              None
            } else {
              Some(tp -> newLeaderAndIsr)
            }
          }

        case None =>
          partitionResponses(tp) = Left(Errors.UNKNOWN_TOPIC_OR_PARTITION)
          None
      }
    }

    // Do the updates in ZK
    debug(s"Updating ISRs for partitions: ${adjustedIsrs.keySet}.")
    val UpdateLeaderAndIsrResult(finishedUpdates, badVersionUpdates) = zkClient.updateLeaderAndIsr(
      adjustedIsrs, controllerContext.epoch, controllerContext.epochZkVersion)

    val successfulUpdates = finishedUpdates.flatMap { case (partition, isrOrError) =>
      isrOrError match {
        case Right(updatedIsr) =>
          debug(s"ISR for partition $partition updated to $updatedIsr.")
          partitionResponses(partition) = Right(updatedIsr)
          Some(partition -> updatedIsr)
        case Left(e) =>
          error(s"Failed to update ISR for partition $partition", e)
          partitionResponses(partition) = Left(Errors.forException(e))
          None
      }
    }

    badVersionUpdates.foreach { partition =>
      info(s"Failed to update ISR to ${adjustedIsrs(partition)} for partition $partition, bad ZK version.")
      partitionResponses(partition) = Left(Errors.INVALID_UPDATE_VERSION)
    }

    // Update our cache and send out metadata updates
    updateLeaderAndIsrCache(successfulUpdates.keys.toSeq)
    sendUpdateMetadataRequest(
      controllerContext.liveOrShuttingDownBrokerIds.toSeq,
      partitionsToAlter.keySet
    )

    partitionResponses.groupBy(_._1.topic).forKeyValue { (topicName, partitionResponses) =>
      // Add each topic part to the response
      val topicResponse = if (useTopicsIds) {
        new AlterPartitionResponseData.TopicData()
          .setTopicId(controllerContext.topicIds.getOrElse(topicName, Uuid.ZERO_UUID))
      } else {
        new AlterPartitionResponseData.TopicData()
          .setTopicName(topicName)
      }
      alterPartitionResponse.topics.add(topicResponse)

      partitionResponses.forKeyValue { (tp, errorOrIsr) =>
        // Add each partition part to the response (new ISR or error)
        errorOrIsr match {
          case Left(error) =>
            topicResponse.partitions.add(
              new AlterPartitionResponseData.PartitionData()
                .setPartitionIndex(tp.partition)
                .setErrorCode(error.code))
          case Right(leaderAndIsr) =>
            /* Setting the LeaderRecoveryState field is always safe because it will always be the same
             * as the value set in the request. For version 0, that is always the default RECOVERED
             * which is ignored when serializing to version 0. For any other version, the
             * LeaderRecoveryState field is supported.
             */
            topicResponse.partitions.add(
              new AlterPartitionResponseData.PartitionData()
                .setPartitionIndex(tp.partition)
                .setLeaderId(leaderAndIsr.leader)
                .setLeaderEpoch(leaderAndIsr.leaderEpoch)
                .setIsr(leaderAndIsr.isr.map(Integer.valueOf).asJava)
                .setLeaderRecoveryState(leaderAndIsr.leaderRecoveryState.value)
                .setPartitionEpoch(leaderAndIsr.partitionEpoch)
            )
        }
      }
    }

    callback(alterPartitionResponse)

    partitionResponses
  }

  def allocateProducerIds(allocateProducerIdsRequest: AllocateProducerIdsRequestData,
                          callback: AllocateProducerIdsResponseData => Unit): Unit = {

    def eventManagerCallback(results: Either[Errors, ProducerIdsBlock]): Unit = {
      results match {
        case Left(error) => callback.apply(new AllocateProducerIdsResponseData().setErrorCode(error.code))
        case Right(pidBlock) => callback.apply(
          new AllocateProducerIdsResponseData()
            .setProducerIdStart(pidBlock.firstProducerId())
            .setProducerIdLen(pidBlock.size()))
      }
    }
    eventManager.put(AllocateProducerIds(allocateProducerIdsRequest.brokerId,
      allocateProducerIdsRequest.brokerEpoch, eventManagerCallback))
  }

  def processAllocateProducerIds(brokerId: Int, brokerEpoch: Long, callback: Either[Errors, ProducerIdsBlock] => Unit): Unit = {
    // Handle a few short-circuits
    if (!isActive) {
      callback.apply(Left(Errors.NOT_CONTROLLER))
      return
    }

    val brokerEpochOpt = controllerContext.liveBrokerIdAndEpochs.get(brokerId)
    if (brokerEpochOpt.isEmpty) {
      warn(s"Ignoring AllocateProducerIds due to unknown broker $brokerId")
      callback.apply(Left(Errors.BROKER_ID_NOT_REGISTERED))
      return
    }

    if (!brokerEpochOpt.contains(brokerEpoch)) {
      warn(s"Ignoring AllocateProducerIds due to stale broker epoch $brokerEpoch for broker $brokerId")
      callback.apply(Left(Errors.STALE_BROKER_EPOCH))
      return
    }

    val maybeNewProducerIdsBlock = try {
      // ** Interacting with zk
      Try(ZkProducerIdManager.getNewProducerIdBlock(brokerId, zkClient, this))
    } catch {
      case ke: KafkaException => Failure(ke)
    }

    maybeNewProducerIdsBlock match {
      case Failure(exception) => callback.apply(Left(Errors.forException(exception)))
      case Success(newProducerIdBlock) => callback.apply(Right(newProducerIdBlock))
    }
  }

  private def processControllerChange(): Unit = {
    maybeResign()
  }

  private def processReelect(): Unit = {
    maybeResign()
    elect()
  }

  private def processRegisterBrokerAndReelect(): Unit = {
    _brokerEpoch = zkClient.registerBroker(brokerInfo)
    processReelect()
  }

  private def processExpire(): Unit = {
    activeControllerId = -1
    onControllerResignation()
  }

  /**
   * Controller处理Event的入口
   * @param event
   */
  override def process(event: ControllerEvent): Unit = {
    try {
      event match {
        // event1: Mock, just test
        case event: MockEvent =>
          // Used only in test cases
          event.process()
        // event2: this event should be handled by ControllerEventThread, so log error
        case ShutdownEventThread =>
          error("Received a ShutdownEventThread event. This type of event is supposed to be handle by ControllerEventThread")
        // event3: Preferred Replica Leader Election
        case AutoPreferredReplicaLeaderElection =>
          processAutoPreferredReplicaLeaderElection()
        // event4: Replica Leader Election
        case ReplicaLeaderElection(partitions, electionType, electionTrigger, callback) =>
          processReplicaLeaderElection(partitions, electionType, electionTrigger, callback)
        // event5: Unclean Leader Election
        case UncleanLeaderElectionEnable =>
          processUncleanLeaderElectionEnable()
        // event6: Unclean Leader Election for specific Topic
        case TopicUncleanLeaderElectionEnable(topic) =>
          processTopicUncleanLeaderElectionEnable(topic)
        // event7: Controller Shutdown
        case ControlledShutdown(id, brokerEpoch, callback) =>
          processControlledShutdown(id, brokerEpoch, callback)
        // event8: Receive Leader and Isr Response
        case LeaderAndIsrResponseReceived(response, brokerId) =>
          processLeaderAndIsrResponseReceived(response, brokerId)
        // event9: Receive Update Metadata Response
        case UpdateMetadataResponseReceived(response, brokerId) =>
          processUpdateMetadataResponseReceived(response, brokerId)
        // event10: Receive topic delete stop replica Response
        case TopicDeletionStopReplicaResponseReceived(replicaId, requestError, partitionErrors) =>
          processTopicDeletionStopReplicaResponseReceived(replicaId, requestError, partitionErrors)
        // event11: Broker Change
        case BrokerChange =>
          processBrokerChange()
        // event12: specific Broker modify
        case BrokerModifications(brokerId) =>
          processBrokerModification(brokerId)
        // event13: Controller Change
        case ControllerChange =>
          processControllerChange()
        // event14: do Reelect
        case Reelect =>
          processReelect()
        // event15: Register Broker And Reelect
        case RegisterBrokerAndReelect =>
          processRegisterBrokerAndReelect()
        // event16: controller Expire
        case Expire =>
          processExpire()
        // event17: topic change
        case TopicChange =>
          processTopicChange()
        // event18: LogDir Event Notification
        case LogDirEventNotification =>
          processLogDirEventNotification()
        // event19: specific Partition Modifications
        case PartitionModifications(topic) =>
          processPartitionModifications(topic)
        // event20: topic delete
        case TopicDeletion =>
          processTopicDeletion()
        // event21: Api Partition Reassignment
        case ApiPartitionReassignment(reassignments, callback) =>
          processApiPartitionReassignment(reassignments, callback)
        // event22: Zk Partition Reassignment
        case ZkPartitionReassignment =>
          processZkPartitionReassignment()
        // event23: List Partition Reassignments
        case ListPartitionReassignments(partitions, callback) =>
          processListPartitionReassignments(partitions, callback)
        // event24: Update Features
        case UpdateFeatures(request, callback) =>
          processFeatureUpdates(request, callback)
        // event25: Isr Change when Partition Reassignment
        case PartitionReassignmentIsrChange(partition) =>
          processPartitionReassignmentIsrChange(partition)
        // event26: Isr Change Notification
        case IsrChangeNotification =>
          processIsrChangeNotification()
        // event27: Receive Alter Partition response
        case AlterPartitionReceived(alterPartitionRequest, alterPartitionRequestVersion, callback) =>
          processAlterPartition(alterPartitionRequest, alterPartitionRequestVersion, callback)
        // event28: Allocate Producer Ids
        case AllocateProducerIds(brokerId, brokerEpoch, callback) =>
          processAllocateProducerIds(brokerId, brokerEpoch, callback)
        // event29: Startup [ KafkaController.Startup() do eventManager.put(Startup) + eventManager.start() ]
        case Startup =>
          processStartup()
      }
    } catch {
      // ControllerMovedException, means the controller role is transferred to another broker
      case e: ControllerMovedException =>
        info(s"Controller moved to another broker when processing $event.", e)
        // try to become controller
        maybeResign()
      case e: Throwable =>
        error(s"Error processing event $event", e)
    } finally {
      updateMetrics()
    }
  }

  override def preempt(event: ControllerEvent): Unit = {
    event.preempt()
  }
}

/**
 * 监听/brokers/ids变更
 * 对应事件为：BrokerChange
 * 对应事件处理方法为：processBrokerChange
 * @param eventManager
 */
class BrokerChangeHandler(eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  // /brokers/ids
  override val path: String = BrokerIdsZNode.path
  // eventManager will put BrokerChange event
  override def handleChildChange(): Unit = {
    eventManager.put(BrokerChange)
  }
}

// zkNode路径：/brokers/ids/$id
// 对应事件：BrokerModifications(brokerId)
// 对应处理函数：processBrokerModification
class BrokerModificationsHandler(eventManager: ControllerEventManager, brokerId: Int) extends ZNodeChangeHandler {
  // ${BrokerIdsZNode.path}/$id
  override val path: String = BrokerIdZNode.path(brokerId)
  // put BrokerModifications(brokerId) to eventManager, KafkaController#processBrokerModification will handle this event
  override def handleDataChange(): Unit = {
    eventManager.put(BrokerModifications(brokerId))
  }
}

// /brokers/topics
class TopicChangeHandler(eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  override val path: String = TopicsZNode.path

  override def handleChildChange(): Unit = eventManager.put(TopicChange)
}

class LogDirEventNotificationHandler(eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  // /log_dir_event_notification
  override val path: String = LogDirEventNotificationZNode.path

  override def handleChildChange(): Unit = eventManager.put(LogDirEventNotification)
}

object LogDirEventNotificationHandler {
  val Version: Long = 1L
}

class PartitionModificationsHandler(eventManager: ControllerEventManager, topic: String) extends ZNodeChangeHandler {
  // /brokers/topics/$topic
  override val path: String = TopicZNode.path(topic)

  override def handleDataChange(): Unit = eventManager.put(PartitionModifications(topic))
}

class TopicDeletionHandler(eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  // /admin/delete_topics
  override val path: String = DeleteTopicsZNode.path

  override def handleChildChange(): Unit = eventManager.put(TopicDeletion)
}

class PartitionReassignmentHandler(eventManager: ControllerEventManager) extends ZNodeChangeHandler {
  // /admin/reassign_partitions
  override val path: String = ReassignPartitionsZNode.path

  // Note that the event is also enqueued when the znode is deleted, but we do it explicitly instead of relying on
  // handleDeletion(). This approach is more robust as it doesn't depend on the watcher being re-registered after
  // it's consumed during data changes (we ensure re-registration when the znode is deleted).
  override def handleCreation(): Unit = eventManager.put(ZkPartitionReassignment)
}

class PartitionReassignmentIsrChangeHandler(eventManager: ControllerEventManager, partition: TopicPartition) extends ZNodeChangeHandler {
  override val path: String = TopicPartitionStateZNode.path(partition)

  override def handleDataChange(): Unit = eventManager.put(PartitionReassignmentIsrChange(partition))
}

class IsrChangeNotificationHandler(eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  // /isr_change_notification
  override val path: String = IsrChangeNotificationZNode.path

  override def handleChildChange(): Unit = eventManager.put(IsrChangeNotification)
}

object IsrChangeNotificationHandler {
  val Version: Long = 1L
}

class PreferredReplicaElectionHandler(eventManager: ControllerEventManager) extends ZNodeChangeHandler {
  // /admin/preferred_replica_election
  override val path: String = PreferredReplicaElectionZNode.path

  override def handleCreation(): Unit = eventManager.put(ReplicaLeaderElection(None, ElectionType.PREFERRED, ZkTriggered))
}

/**
 * listener: listen the change of controller
 * 用于监听/controller下的变更
 * @param eventManager
 */
class ControllerChangeHandler(eventManager: ControllerEventManager) extends ZNodeChangeHandler {
  // "/controller"
  override val path: String = ControllerZNode.path
  // when node is created -> means someone broker become the leader -> handle ControllerChange event
  override def handleCreation(): Unit = eventManager.put(ControllerChange)
  // when node is deleted -> means no leader exist-> handle Reelect event
  override def handleDeletion(): Unit = eventManager.put(Reelect)
  // when node is changed -> means leader maybe changed -> handle ControllerChange event
  override def handleDataChange(): Unit = eventManager.put(ControllerChange)
}

case class PartitionAndReplica(topicPartition: TopicPartition, replica: Int) {
  def topic: String = topicPartition.topic
  def partition: Int = topicPartition.partition

  override def toString: String = {
    s"[Topic=$topic,Partition=$partition,Replica=$replica]"
  }
}

case class LeaderIsrAndControllerEpoch(leaderAndIsr: LeaderAndIsr, controllerEpoch: Int) {
  override def toString: String = {
    val leaderAndIsrInfo = new StringBuilder
    leaderAndIsrInfo.append("(Leader:" + leaderAndIsr.leader)
    leaderAndIsrInfo.append(",ISR:" + leaderAndIsr.isr.mkString(","))
    leaderAndIsrInfo.append(",LeaderRecoveryState:" + leaderAndIsr.leaderRecoveryState)
    leaderAndIsrInfo.append(",LeaderEpoch:" + leaderAndIsr.leaderEpoch)
    leaderAndIsrInfo.append(",ZkVersion:" + leaderAndIsr.partitionEpoch)
    leaderAndIsrInfo.append(",ControllerEpoch:" + controllerEpoch + ")")
    leaderAndIsrInfo.toString()
  }
}

private[controller] class ControllerStats {
  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  // per second "unclean leader" occurring
  val uncleanLeaderElectionRate = metricsGroup.newMeter("UncleanLeaderElectionsPerSec", "elections", TimeUnit.SECONDS)

  val rateAndTimeMetrics: Map[ControllerState, Timer] = ControllerState.values.flatMap { state =>
    state.rateAndTimeMetricName.map { metricName =>
      state -> metricsGroup.newTimer(metricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS)
    }
  }.toMap

  // For test.
  def removeMetric(name: String): Unit = {
    metricsGroup.removeMetric(name)
  }
}

sealed trait ControllerEvent {
  // state
  def state: ControllerState
  // preempt() is not executed by `ControllerEventThread` but by the main thread.
  def preempt(): Unit
}

case object ControllerChange extends ControllerEvent {
  override def state: ControllerState = ControllerState.ControllerChange
  override def preempt(): Unit = {}
}

case object Reelect extends ControllerEvent {
  override def state: ControllerState = ControllerState.ControllerChange
  override def preempt(): Unit = {}
}

case object RegisterBrokerAndReelect extends ControllerEvent {
  override def state: ControllerState = ControllerState.ControllerChange
  override def preempt(): Unit = {}
}

case object Expire extends ControllerEvent {
  override def state: ControllerState = ControllerState.ControllerChange
  override def preempt(): Unit = {}
}

case object ShutdownEventThread extends ControllerEvent {
  override def state: ControllerState = ControllerState.ControllerShutdown
  override def preempt(): Unit = {}
}

case object AutoPreferredReplicaLeaderElection extends ControllerEvent {
  override def state: ControllerState = ControllerState.AutoLeaderBalance
  override def preempt(): Unit = {}
}

case object UncleanLeaderElectionEnable extends ControllerEvent {
  override def state: ControllerState = ControllerState.UncleanLeaderElectionEnable
  override def preempt(): Unit = {}
}

case class TopicUncleanLeaderElectionEnable(topic: String) extends ControllerEvent {
  override def state: ControllerState = ControllerState.TopicUncleanLeaderElectionEnable
  override def preempt(): Unit = {}
}

case class ControlledShutdown(id: Int, brokerEpoch: Long, controlledShutdownCallback: Try[Set[TopicPartition]] => Unit) extends ControllerEvent {
  override def state: ControllerState = ControllerState.ControlledShutdown
  override def preempt(): Unit = controlledShutdownCallback(Failure(new ControllerMovedException("Controller moved to another broker")))
}

case class LeaderAndIsrResponseReceived(leaderAndIsrResponse: LeaderAndIsrResponse, brokerId: Int) extends ControllerEvent {
  override def state: ControllerState = ControllerState.LeaderAndIsrResponseReceived
  override def preempt(): Unit = {}
}

case class UpdateMetadataResponseReceived(updateMetadataResponse: UpdateMetadataResponse, brokerId: Int) extends ControllerEvent {
  override def state: ControllerState = ControllerState.UpdateMetadataResponseReceived
  override def preempt(): Unit = {}
}

case class TopicDeletionStopReplicaResponseReceived(replicaId: Int,
                                                    requestError: Errors,
                                                    partitionErrors: Map[TopicPartition, Errors]) extends ControllerEvent {
  override def state: ControllerState = ControllerState.TopicDeletion
  override def preempt(): Unit = {}
}

case object Startup extends ControllerEvent {
  override def state: ControllerState = ControllerState.ControllerChange
  override def preempt(): Unit = {}
}

case object BrokerChange extends ControllerEvent {
  override def state: ControllerState = ControllerState.BrokerChange
  override def preempt(): Unit = {}
}

case class BrokerModifications(brokerId: Int) extends ControllerEvent {
  override def state: ControllerState = ControllerState.BrokerChange
  override def preempt(): Unit = {}
}

case object TopicChange extends ControllerEvent {
  override def state: ControllerState = ControllerState.TopicChange
  override def preempt(): Unit = {}
}

case object LogDirEventNotification extends ControllerEvent {
  override def state: ControllerState = ControllerState.LogDirChange
  override def preempt(): Unit = {}
}

case class PartitionModifications(topic: String) extends ControllerEvent {
  override def state: ControllerState = ControllerState.TopicChange
  override def preempt(): Unit = {}
}

case object TopicDeletion extends ControllerEvent {
  override def state: ControllerState = ControllerState.TopicDeletion
  override def preempt(): Unit = {}
}

case object ZkPartitionReassignment extends ControllerEvent {
  override def state: ControllerState = ControllerState.AlterPartitionReassignment
  override def preempt(): Unit = {}
}

case class ApiPartitionReassignment(reassignments: Map[TopicPartition, Option[Seq[Int]]],
                                    callback: AlterReassignmentsCallback) extends ControllerEvent {
  override def state: ControllerState = ControllerState.AlterPartitionReassignment
  override def preempt(): Unit = callback(Right(new ApiError(Errors.NOT_CONTROLLER)))
}

case class PartitionReassignmentIsrChange(partition: TopicPartition) extends ControllerEvent {
  override def state: ControllerState = ControllerState.AlterPartitionReassignment
  override def preempt(): Unit = {}
}

case object IsrChangeNotification extends ControllerEvent {
  override def state: ControllerState = ControllerState.IsrChange
  override def preempt(): Unit = {}
}

case class AlterPartitionReceived(
  alterPartitionRequest: AlterPartitionRequestData,
  alterPartitionRequestVersion: Short,
  callback: AlterPartitionResponseData => Unit
) extends ControllerEvent {
  override def state: ControllerState = ControllerState.IsrChange
  override def preempt(): Unit = {}
}

case class ReplicaLeaderElection(
  partitionsFromAdminClientOpt: Option[Set[TopicPartition]],
  electionType: ElectionType,
  electionTrigger: ElectionTrigger,
  callback: ElectLeadersCallback = _ => {}
) extends ControllerEvent {
  override def state: ControllerState = ControllerState.ManualLeaderBalance

  override def preempt(): Unit = callback(
    partitionsFromAdminClientOpt.fold(Map.empty[TopicPartition, Either[ApiError, Int]]) { partitions =>
      partitions.iterator.map(partition => partition -> Left(new ApiError(Errors.NOT_CONTROLLER, null))).toMap
    }
  )
}

/**
  * @param partitionsOpt - an Optional set of partitions. If not present, all reassigning partitions are to be listed
  */
case class ListPartitionReassignments(partitionsOpt: Option[Set[TopicPartition]],
                                      callback: ListReassignmentsCallback) extends ControllerEvent {
  override def state: ControllerState = ControllerState.ListPartitionReassignment
  override def preempt(): Unit = callback(Right(new ApiError(Errors.NOT_CONTROLLER, null)))
}

case class UpdateFeatures(request: UpdateFeaturesRequest,
                          callback: UpdateFeaturesCallback) extends ControllerEvent {
  override def state: ControllerState = ControllerState.UpdateFeatures
  override def preempt(): Unit = {}
}

case class AllocateProducerIds(brokerId: Int, brokerEpoch: Long, callback: Either[Errors, ProducerIdsBlock] => Unit)
    extends ControllerEvent {
  override def state: ControllerState = ControllerState.Idle
  override def preempt(): Unit = {}
}


// Used only in test cases
abstract class MockEvent(val state: ControllerState) extends ControllerEvent {
  def process(): Unit
  def preempt(): Unit
}
