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

import com.yammer.metrics.core.{Gauge, Timer}
import kafka.api._
import kafka.cluster.Broker
import kafka.server.KafkaConfig
import kafka.utils.Implicits._
import kafka.utils._
import org.apache.kafka.clients._
import org.apache.kafka.common._
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.StopReplicaRequestData.{StopReplicaPartitionState, StopReplicaTopicState}
import org.apache.kafka.common.message.UpdateMetadataRequestData.{UpdateMetadataBroker, UpdateMetadataEndpoint, UpdateMetadataPartitionState}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.common.MetadataVersion._
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.util.ShutdownableThread

import java.net.SocketTimeoutException
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}
import scala.collection.{Seq, Set, mutable}
import scala.jdk.CollectionConverters._

object ControllerChannelManager {
  val QueueSizeMetricName = "QueueSize"
  val RequestRateAndQueueTimeMetricName = "RequestRateAndQueueTimeMs"
}

class ControllerChannelManager(controllerEpoch: () => Int,
                               config: KafkaConfig,
                               time: Time,
                               metrics: Metrics,
                               stateChangeLogger: StateChangeLogger,
                               threadNamePrefix: Option[String] = None) extends Logging {
  import ControllerChannelManager._

  private val metricsGroup = new KafkaMetricsGroup(this.getClass)
  // * ControllerBrokerStateInfo contain the RequestSendThread
  // 最重要的对象，不要被名字所迷惑，可不仅仅是State信息，Map结构：
  // key-目标连接的成员brokerID，
  // value-对应目标Broker的相关信息的封装
  // - 发给谁：brokerNode - 目标broker的连接地址等信息
  // - 发什么：messageQueue - 准备发给目标的“Controller” request
  // - 谁来发：requestSendThread - 提供了发送的能力
  protected val brokerStateInfo = new mutable.HashMap[Int, ControllerBrokerStateInfo]

  // 对象锁，下面大部分方法都加了锁
  private val brokerLock = new Object

  this.logIdent = "[Channel manager on controller " + config.brokerId + "]: "

  metricsGroup.newGauge("TotalQueueSize",
    () => brokerLock synchronized {
      brokerStateInfo.values.iterator.map(_.messageQueue.size).sum
    }
  )

  def startup(initialBrokers: Set[Broker]):Unit = {
    // 1. add each broker to brokerStateInfo
    // 把每个broker加入到 brokerStateInfo这一Map结构
    initialBrokers.foreach(addNewBroker)
    // 2. start RequestSendThread for each broker
    // 对应的RequestSendThread开始循环doWork
    brokerLock synchronized {
      brokerStateInfo.foreach(brokerState => startRequestSendThread(brokerState._1))
    }
  }

  def shutdown():Unit = {
    brokerLock synchronized {
      brokerStateInfo.values.toList.foreach(removeExistingBroker)
    }
  }

  /**
   * send Request to specific broker BlockingQueue[QueueItem]
   * 将Controller Request 放入 BlockingQueue
   * @param brokerId
   * @param request
   * @param callback
   */
  def sendRequest(brokerId: Int, request: AbstractControlRequest.Builder[_ <: AbstractControlRequest],
                  callback: AbstractResponse => Unit = null): Unit = {
    brokerLock synchronized {
      // 1. get broker state
      val stateInfoOpt = brokerStateInfo.get(brokerId)
      stateInfoOpt match {
        // 2. If the status matches, do send to queue
        case Some(stateInfo) =>
          stateInfo.messageQueue.put(QueueItem(request.apiKey, request, callback, time.milliseconds()))
        case None =>
          warn(s"Not sending request ${request.apiKey.name} with controllerId=${request.controllerId()}, " +
            s"controllerEpoch=${request.controllerEpoch()}, brokerEpoch=${request.brokerEpoch()} " +
            s"to broker $brokerId, since it is offline.")
      }
    }
  }

  /**
   * when a broker join the cluster, controller will can this method to "Bring brokers into management”
   * @param broker
   */
  def addBroker(broker: Broker): Unit = {
    // be careful here. Maybe the startup() API has already started the request send thread
    brokerLock synchronized {
      if (!brokerStateInfo.contains(broker.id)) {
        // 1. step 1: Prepare network and thread settings
        // "Controller-${config.brokerId}-to-broker-${broker.id}-send-thread"
        addNewBroker(broker)
        // 2. step 2: start RequestSendThread for this broker
        startRequestSendThread(broker.id)
      }
    }
  }

  /**
   * remove specific broker
   * @param brokerId
   */
  def removeBroker(brokerId: Int): Unit = {
    brokerLock synchronized {
      removeExistingBroker(brokerStateInfo(brokerId))
    }
  }

  private def addNewBroker(broker: Broker): Unit = {
    // 1. create queue to store message
    val messageQueue = new LinkedBlockingQueue[QueueItem]
    debug(s"Controller ${config.brokerId} trying to connect to broker ${broker.id}")

    // 2. get Communication <ListenerName + Protocol> between controller and broker
    val controllerToBrokerListenerName = config.controlPlaneListenerName.getOrElse(config.interBrokerListenerName)
    val controllerToBrokerSecurityProtocol = config.controlPlaneSecurityProtocol.getOrElse(config.interBrokerSecurityProtocol)

    // 3. Build a connection to the broker
    val brokerNode = broker.node(controllerToBrokerListenerName)
    val logContext = new LogContext(s"[Controller id=${config.brokerId}, targetBrokerId=${brokerNode.idString}] ")

    // 4. build network client and reconfigurable channelBuilder
    val (networkClient, reconfigurableChannelBuilder) = {
      // 4.1 build channelBuilder
      val channelBuilder = ChannelBuilders.clientChannelBuilder(
        controllerToBrokerSecurityProtocol,
        JaasContext.Type.SERVER,
        config,
        controllerToBrokerListenerName,
        config.saslMechanismInterBrokerProtocol,
        time,
        config.saslInterBrokerHandshakeRequestEnable,
        logContext
      )
      // 4.2 if channelBuilder support reconfigurable
      val reconfigurableChannelBuilder = channelBuilder match {
        case reconfigurable: Reconfigurable =>
          config.addReconfigurable(reconfigurable)
          Some(reconfigurable)
        case _ => None
      }
      // 4.3 build Selector
      val selector = new Selector(
        NetworkReceive.UNLIMITED,
        Selector.NO_IDLE_TIMEOUT_MS,
        metrics,
        time,
        "controller-channel",
        Map("broker-id" -> brokerNode.idString).asJava,
        false,
        channelBuilder,
        logContext
      )
      // 4.4 build networkClient
      val networkClient = new NetworkClient(
        selector,
        new ManualMetadataUpdater(Seq(brokerNode).asJava),
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
        logContext
      )
      // 4.5 return
      (networkClient, reconfigurableChannelBuilder)
    }

    // 5. Build RequestSendThread
    // 5.1 RequestSendThread name：线程名
    val threadName = threadNamePrefix match {
      case None => s"Controller-${config.brokerId}-to-broker-${broker.id}-send-thread"
      case Some(name) => s"$name:Controller-${config.brokerId}-to-broker-${broker.id}-send-thread"
    }
    // 5.2 Metrics
    val requestRateAndQueueTimeMetrics = metricsGroup.newTimer(
      RequestRateAndQueueTimeMetricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS, brokerMetricTags(broker.id)
    )
    // 5.3 build RequestSendThread and set unDaemon
    val requestThread = new RequestSendThread(config.brokerId, controllerEpoch, messageQueue, networkClient,
      brokerNode, config, time, requestRateAndQueueTimeMetrics, stateChangeLogger, threadName)
    requestThread.setDaemon(false)

    // 6. queueSize metrics
    val queueSizeGauge = metricsGroup.newGauge(QueueSizeMetricName, () => messageQueue.size, brokerMetricTags(broker.id))

    // 7. add the "new" broker info to brokerStateInfo
    // 更新brokerStateInfo中的Map结构：由这里就可以看出[ messageQueue ]和 [requestThread]都是每个目标broker独立的
    brokerStateInfo.put(broker.id, ControllerBrokerStateInfo(networkClient, brokerNode, messageQueue,
      requestThread, queueSizeGauge, requestRateAndQueueTimeMetrics, reconfigurableChannelBuilder))
  }

  private def brokerMetricTags(brokerId: Int) = Map("broker-id" -> brokerId.toString).asJava

  /**
   * 清除维护的相关“信息”
   * @param brokerState
   */
  private def removeExistingBroker(brokerState: ControllerBrokerStateInfo): Unit = {
    try {
      // Shutdown the RequestSendThread before closing the NetworkClient to avoid the concurrent use of the
      // non-threadsafe classes as described in KAFKA-4959.
      // The call to shutdownLatch.await() in ShutdownableThread.shutdown() serves as a synchronization barrier that
      // hands off the NetworkClient from the RequestSendThread to the ZkEventThread.

      // 1. remove reconfigurableChannelBuilder
      brokerState.reconfigurableChannelBuilder.foreach(config.removeReconfigurable)
      // 2. shutdown requestSendThread
      brokerState.requestSendThread.shutdown()
      // 3. close networkClient
      brokerState.networkClient.close()
      // 4. clear up the messageQueue
      brokerState.messageQueue.clear()

      // 5. remove metrics
      metricsGroup.removeMetric(QueueSizeMetricName, brokerMetricTags(brokerState.brokerNode.id))
      metricsGroup.removeMetric(RequestRateAndQueueTimeMetricName, brokerMetricTags(brokerState.brokerNode.id))

      // remove this broker from brokerStateInfo
      brokerStateInfo.remove(brokerState.brokerNode.id)
    } catch {
      case e: Throwable => error("Error while removing broker by the controller", e)
    }
  }

  /**
   * start requestSend Thread for specific broker
   * @param brokerId
   */
  protected def startRequestSendThread(brokerId: Int): Unit = {
    // 1. get RequestSendThread from brokerStateInfo by brokerId
    val requestThread = brokerStateInfo(brokerId).requestSendThread
    // 2. if Thread.State is NEW, will start the RequestSendThread
    if (requestThread.getState == Thread.State.NEW) {
      // start...
      requestThread.start()
    }
  }
}

/**
 * 定义了往“网络请求阻塞”队列中写入什么样的Request， 必须是继承AbstractControlRequest的Request
 * - UpdateMetadataRequest
 * - LeaderAndIsrRequest
 * - StopReplicaRequest
 * @param apiKey: Kafka APIs
 * @param request: request must extends AbstractControlRequest
 * @param callback
 * @param enqueueTimeMs
 */
case class QueueItem(apiKey: ApiKeys, request: AbstractControlRequest.Builder[_ <: AbstractControlRequest],
                     callback: AbstractResponse => Unit, enqueueTimeMs: Long)

class RequestSendThread(val controllerId: Int, // the broker ID of current controller
                        controllerEpoch: () => Int, // current controller Epoch
                        val queue: BlockingQueue[QueueItem], // BlockingQueue to save (controller) request
                        val networkClient: NetworkClient, // kafka自己封装的网络客户端
                        val brokerNode: Node, // target broker
                        val config: KafkaConfig,
                        val time: Time,
                        val requestRateAndQueueTimeMetrics: Timer,
                        val stateChangeLogger: StateChangeLogger,
                        name: String)
  extends ShutdownableThread(name, true, s"[RequestSendThread controllerId=$controllerId] ")
    with Logging {

  logIdent = logPrefix

  private val socketTimeoutMs = config.controllerSocketTimeoutMs

  /**
   * RequestSendThread: 循环处理调用的方法
   * 核心流程：
   * 1. 从queue中取出Request
   * 2. 发送Request -> 阻塞 -> 直到收到Response 或 发现断联
   * 3. 收到Response -> 执行callback
   */
  override def doWork(): Unit = {

    def backoff(): Unit = pause(100, TimeUnit.MILLISECONDS)
    // 1. get (controller) Request from BlockingQueue
    val QueueItem(apiKey, requestBuilder, callback, enqueueTimeMs) = queue.take()
    // 2. update TimeMetrics
    requestRateAndQueueTimeMetrics.update(time.milliseconds() - enqueueTimeMs, TimeUnit.MILLISECONDS)

    var clientResponse: ClientResponse = null
    try {
      var isSendSuccessful = false
      // 3. when isRunning and send fail, will always be executed
      while (isRunning && !isSendSuccessful) {
        // if a broker goes down for a long time, then at some point the controller's zookeeper listener will trigger a
        // removeBroker which will invoke shutdown() on this thread. At that point, we will stop retrying.
        try {
          // 3.1 target broker is not Ready
          if (!brokerReady()) {
            // mark fail and Waiting for retry
            isSendSuccessful = false
            backoff()
          }
          else {
          // 3.2 target broker is Ready
            // construct Request
            val clientRequest = networkClient.newClientRequest(brokerNode.idString, requestBuilder,
              time.milliseconds(), true)
            // send Request and wait Response
            // 发送-阻塞-直到收到响应 或 发现断联
            // controller 接收到response的地方
            clientResponse = NetworkClientUtils.sendAndReceive(networkClient, clientRequest, time)
            isSendSuccessful = true
          }
        } catch {
        // 3.3 find Exception
          case e: Throwable => // if the send was not successful, reconnect to broker and resend the message
            warn(s"Controller $controllerId epoch ${controllerEpoch()} fails to send request " +
              s"$requestBuilder " +
              s"to broker $brokerNode. Reconnecting to broker.", e)
            // close connection with broker
            networkClient.close(brokerNode.idString)
            // mark fail and Waiting for retry
            isSendSuccessful = false
            backoff()
        }
      }
    // 4. get the Response
      if (clientResponse != null) {
        // 4.1 get requestHeader
        val requestHeader = clientResponse.requestHeader
        // 4.2 Determine whether it is a specific type three request
        val api = requestHeader.apiKey
        if (api != ApiKeys.LEADER_AND_ISR && api != ApiKeys.STOP_REPLICA && api != ApiKeys.UPDATE_METADATA)
          throw new KafkaException(s"Unexpected apiKey received: $apiKey")
        // 4.3 get the Response
        val response = clientResponse.responseBody

        stateChangeLogger.withControllerEpoch(controllerEpoch()).trace(s"Received response " +
          s"$response for request $api with correlation id " +
          s"${requestHeader.correlationId} sent to broker $brokerNode")
        // 4.4 handle callback
        // controller 利用response 来执行回调的地方
        if (callback != null) {
          callback(response)
        }
      }
    } catch {
      case e: Throwable =>
        error(s"Controller $controllerId fails to send a request to broker $brokerNode", e)
        // If there is any socket error (eg, socket timeout), the connection is no longer usable and needs to be recreated.
        networkClient.close(brokerNode.idString)
    }
  }

  private def brokerReady(): Boolean = {
    try {
      if (!NetworkClientUtils.isReady(networkClient, brokerNode, time.milliseconds())) {
        if (!NetworkClientUtils.awaitReady(networkClient, brokerNode, time, socketTimeoutMs))
          throw new SocketTimeoutException(s"Failed to connect within $socketTimeoutMs ms")

        info(s"Controller $controllerId connected to $brokerNode for sending state change requests")
      }

      true
    } catch {
      case e: Throwable =>
        warn(s"Controller $controllerId's connection to broker $brokerNode was unsuccessful", e)
        networkClient.close(brokerNode.idString)
        false
    }
  }

  override def initiateShutdown(): Boolean = {
    if (super.initiateShutdown()) {
      networkClient.initiateClose()
      true
    } else
      false
  }
}

class ControllerBrokerRequestBatch(
  config: KafkaConfig,
  controllerChannelManager: ControllerChannelManager,
  controllerEventManager: ControllerEventManager,
  controllerContext: ControllerContext,
  stateChangeLogger: StateChangeLogger
) extends AbstractControllerBrokerRequestBatch(
  config,
  () => controllerContext,
  () => config.interBrokerProtocolVersion,
  stateChangeLogger
) {

  def sendEvent(event: ControllerEvent): Unit = {
    controllerEventManager.put(event)
  }
  def sendRequest(brokerId: Int,
                  request: AbstractControlRequest.Builder[_ <: AbstractControlRequest],
                  callback: AbstractResponse => Unit = null): Unit = {
    controllerChannelManager.sendRequest(brokerId, request, callback)
  }
  // LeaderAndIsrResponse 执行的回调函数，居然也放入一个Event | LeaderAndIsrResponseReceived
  override def handleLeaderAndIsrResponse(response: LeaderAndIsrResponse, broker: Int): Unit = {
    sendEvent(LeaderAndIsrResponseReceived(response, broker))
  }

  // UpdateMetadataResponse 执行的回调函数, 都是 Event机制
  override def handleUpdateMetadataResponse(response: UpdateMetadataResponse, broker: Int): Unit = {
    sendEvent(UpdateMetadataResponseReceived(response, broker))
  }
  // StopReplicaResponse 执行的回调函数
  override def handleStopReplicaResponse(stopReplicaResponse: StopReplicaResponse, brokerId: Int,
                                         partitionErrorsForDeletingTopics: Map[TopicPartition, Errors]): Unit = {
    if (partitionErrorsForDeletingTopics.nonEmpty) {
      // StopReplica Response callback : put event to controllerEventManager
      // 会走到 TopicDeletionStopReplicaResponseReceived 事件的处理
      sendEvent(TopicDeletionStopReplicaResponseReceived(brokerId, stopReplicaResponse.error, partitionErrorsForDeletingTopics))
    }
  }
}

/**
 * Structure to send RPCs from controller to broker to inform about the metadata and leadership
 * changes in the system.
 * @param config Kafka config present in the controller.
 * @param metadataProvider Provider to provide the relevant metadata to build the state needed to
 *                         send RPCs
 * @param metadataVersionProvider Provider to provide the metadata version used by the controller.
 * @param stateChangeLogger logger to log the various events while sending requests and receiving
 *                          responses from the brokers
 * @param kraftController whether the controller is KRaft controller
 */
abstract class AbstractControllerBrokerRequestBatch(config: KafkaConfig,
                                                    metadataProvider: () => ControllerChannelContext,
                                                    metadataVersionProvider: () => MetadataVersion,
                                                    stateChangeLogger: StateChangeLogger,
                                                    kraftController: Boolean = false) extends Logging {
  val controllerId: Int = config.brokerId
  val leaderAndIsrRequestMap = mutable.Map.empty[Int, mutable.Map[TopicPartition, LeaderAndIsrPartitionState]]
  val stopReplicaRequestMap = mutable.Map.empty[Int, mutable.Map[TopicPartition, StopReplicaPartitionState]]
  val updateMetadataRequestBrokerSet = mutable.Set.empty[Int]
  val updateMetadataRequestPartitionInfoMap = mutable.Map.empty[TopicPartition, UpdateMetadataPartitionState]
  private var updateType: AbstractControlRequest.Type = AbstractControlRequest.Type.UNKNOWN
  private var metadataInstance: ControllerChannelContext = _

  def sendRequest(brokerId: Int,
                  request: AbstractControlRequest.Builder[_ <: AbstractControlRequest],
                  callback: AbstractResponse => Unit = null): Unit

  def newBatch(): Unit = {
    // raise error if the previous batch is not empty
    if (leaderAndIsrRequestMap.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating " +
        s"a new one. Some LeaderAndIsr state changes $leaderAndIsrRequestMap might be lost ")
    if (stopReplicaRequestMap.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        s"new one. Some StopReplica state changes $stopReplicaRequestMap might be lost ")
    if (updateMetadataRequestBrokerSet.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        s"new one. Some UpdateMetadata state changes to brokers $updateMetadataRequestBrokerSet with partition info " +
        s"$updateMetadataRequestPartitionInfoMap might be lost ")
    metadataInstance = metadataProvider()
  }

  def setUpdateType(updateType: AbstractControlRequest.Type): Unit = {
    this.updateType = updateType
  }

  def clear(): Unit = {
    leaderAndIsrRequestMap.clear()
    stopReplicaRequestMap.clear()
    updateMetadataRequestBrokerSet.clear()
    updateMetadataRequestPartitionInfoMap.clear()
    metadataInstance = null
    updateType = AbstractControlRequest.Type.UNKNOWN
  }

  /**
   * 1. send LeaderAndIsrRequest to corresponding broker
   * 2. send UpdateMetadataRequest to all Brokers
   */
  def addLeaderAndIsrRequestForBrokers(brokerIds: Seq[Int],
                                       topicPartition: TopicPartition,
                                       leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch,
                                       replicaAssignment: ReplicaAssignment,
                                       isNew: Boolean): Unit = {
    // 1. send LeaderAndIsrRequest to corresponding broker
    brokerIds.filter(_ >= 0).foreach { brokerId =>
      val result = leaderAndIsrRequestMap.getOrElseUpdate(brokerId, mutable.Map.empty)
      val alreadyNew = result.get(topicPartition).exists(_.isNew)
      val leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr
      val partitionState = new LeaderAndIsrPartitionState()
        .setTopicName(topicPartition.topic)
        .setPartitionIndex(topicPartition.partition)
        .setControllerEpoch(leaderIsrAndControllerEpoch.controllerEpoch)
        .setLeader(leaderAndIsr.leader)
        .setLeaderEpoch(leaderAndIsr.leaderEpoch)
        .setIsr(leaderAndIsr.isr.map(Integer.valueOf).asJava)
        .setPartitionEpoch(leaderAndIsr.partitionEpoch)
        .setReplicas(replicaAssignment.replicas.map(Integer.valueOf).asJava)
        .setAddingReplicas(replicaAssignment.addingReplicas.map(Integer.valueOf).asJava)
        .setRemovingReplicas(replicaAssignment.removingReplicas.map(Integer.valueOf).asJava)
        .setIsNew(isNew || alreadyNew)

      if (metadataVersionProvider.apply().isAtLeast(IBP_3_2_IV0)) {
        partitionState.setLeaderRecoveryState(leaderAndIsr.leaderRecoveryState.value)
      }

      result.put(topicPartition, partitionState)
    }
    // 2. send UpdateMetadataRequest to all Brokers
    // UpdateMetadataRequest场景示例
    addUpdateMetadataRequestForBrokers(metadataInstance.liveOrShuttingDownBrokerIds.toSeq, Set(topicPartition))
  }

  def addStopReplicaRequestForBrokers(brokerIds: Seq[Int],
                                      topicPartition: TopicPartition,
                                      deletePartition: Boolean): Unit = {
    // A sentinel (-2) is used as an epoch if the topic is queued for deletion. It overrides
    // any existing epoch.
    val leaderEpoch = metadataInstance.leaderEpoch(topicPartition)

    brokerIds.filter(_ >= 0).foreach { brokerId =>
      val result = stopReplicaRequestMap.getOrElseUpdate(brokerId, mutable.Map.empty)
      val alreadyDelete = result.get(topicPartition).exists(_.deletePartition)
      result.put(topicPartition, new StopReplicaPartitionState()
          .setPartitionIndex(topicPartition.partition())
          .setLeaderEpoch(leaderEpoch)
          .setDeletePartition(alreadyDelete || deletePartition))
    }
  }

  /**
   * Controller 往 成员 Broker 发送 UpdateMetadataRequest， 构造 Request 的地方
   * Send UpdateMetadataRequest to the given brokers for the given partitions and partitions that are being deleted */
  def addUpdateMetadataRequestForBrokers(brokerIds: Seq[Int],
                                         partitions: collection.Set[TopicPartition]): Unit = {
    updateMetadataRequestBrokerSet ++= brokerIds.filter(_ >= 0)
    partitions.foreach { partition =>
      // 从 metadataInstance: ControllerChannelContext 中获取信息
      val beingDeleted = metadataInstance.isTopicQueuedUpForDeletion(partition.topic())
      metadataInstance.partitionLeadershipInfo(partition) match {
        case Some(LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)) =>
          val replicas = metadataInstance.partitionReplicaAssignment(partition)
          val offlineReplicas = replicas.filter(!metadataInstance.isReplicaOnline(_, partition))
          val updatedLeaderAndIsr =
            if (beingDeleted) LeaderAndIsr.duringDelete(leaderAndIsr.isr)
            else leaderAndIsr

         // 利用 从 metadataInstance 中获取的信息，填充构造 UpdateMetadataRequest
          addUpdateMetadataRequestForBrokers(brokerIds, controllerEpoch, partition,
            updatedLeaderAndIsr.leader, updatedLeaderAndIsr.leaderEpoch, updatedLeaderAndIsr.partitionEpoch,
            updatedLeaderAndIsr.isr, replicas, offlineReplicas)
        case None =>
          info(s"Leader not yet assigned for partition $partition. Skip sending UpdateMetadataRequest.")
      }
    }
  }

  def addUpdateMetadataRequestForBrokers(brokerIds: Seq[Int]): Unit = {
    updateMetadataRequestBrokerSet ++= brokerIds.filter(_ >= 0)
  }

  def addUpdateMetadataRequestForBrokers(brokerIds: Seq[Int],
                                         controllerEpoch: Int,
                                         partition: TopicPartition,
                                         leader: Int,
                                         leaderEpoch: Int,
                                         partitionEpoch: Int,
                                         isrs: List[Int],
                                         replicas: Seq[Int],
                                         offlineReplicas: Seq[Int]): Unit = {
    updateMetadataRequestBrokerSet ++= brokerIds.filter(_ >= 0)
    val partitionStateInfo = new UpdateMetadataPartitionState()
      .setTopicName(partition.topic)
      .setPartitionIndex(partition.partition)
      .setControllerEpoch(controllerEpoch)
      .setLeader(leader)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isrs.map(Integer.valueOf).asJava)
      .setZkVersion(partitionEpoch)
      .setReplicas(replicas.map(Integer.valueOf).asJava)
      .setOfflineReplicas(offlineReplicas.map(Integer.valueOf).asJava)
    updateMetadataRequestPartitionInfoMap.put(partition, partitionStateInfo)
  }

  private def sendLeaderAndIsrRequest(controllerEpoch: Int, stateChangeLog: StateChangeLogger): Unit = {
    val metadataVersion = metadataVersionProvider.apply()
    val leaderAndIsrRequestVersion: Short =
      if (metadataVersion.isAtLeast(IBP_3_4_IV0)) 7
      else if (metadataVersion.isAtLeast(IBP_3_2_IV0)) 6
      else if (metadataVersion.isAtLeast(IBP_2_8_IV1)) 5
      else if (metadataVersion.isAtLeast(IBP_2_4_IV1)) 4
      else if (metadataVersion.isAtLeast(IBP_2_4_IV0)) 3
      else if (metadataVersion.isAtLeast(IBP_2_2_IV0)) 2
      else if (metadataVersion.isAtLeast(IBP_1_0_IV0)) 1
      else 0

    leaderAndIsrRequestMap.forKeyValue { (broker, leaderAndIsrPartitionStates) =>
      if (metadataInstance.liveOrShuttingDownBrokerIds.contains(broker)) {
        val leaderIds = mutable.Set.empty[Int]
        var numBecomeLeaders = 0
        leaderAndIsrPartitionStates.forKeyValue { (topicPartition, state) =>
          leaderIds += state.leader
          val typeOfRequest = if (broker == state.leader) {
            numBecomeLeaders += 1
            "become-leader"
          } else {
            "become-follower"
          }
          if (stateChangeLog.isTraceEnabled)
            stateChangeLog.trace(s"Sending $typeOfRequest LeaderAndIsr request $state to broker $broker for partition $topicPartition")
        }
        stateChangeLog.info(s"Sending LeaderAndIsr request to broker $broker with $numBecomeLeaders become-leader " +
          s"and ${leaderAndIsrPartitionStates.size - numBecomeLeaders} become-follower partitions")
        val leaders = metadataInstance.liveOrShuttingDownBrokers.filter(b => leaderIds.contains(b.id)).map {
          _.node(config.interBrokerListenerName)
        }
        val brokerEpoch = metadataInstance.liveBrokerIdAndEpochs(broker)
        val topicIds = leaderAndIsrPartitionStates.keys
          .map(_.topic)
          .toSet[String]
          .map(topic => (topic, metadataInstance.topicIds.getOrElse(topic, Uuid.ZERO_UUID)))
          .toMap
        val leaderAndIsrRequestBuilder = new LeaderAndIsrRequest.Builder(
          leaderAndIsrRequestVersion,
          controllerId,
          controllerEpoch,
          brokerEpoch,
          leaderAndIsrPartitionStates.values.toBuffer.asJava,
          topicIds.asJava,
          leaders.asJava,
          kraftController,
          updateType
        )
        sendRequest(broker, leaderAndIsrRequestBuilder, (r: AbstractResponse) => {
          val leaderAndIsrResponse = r.asInstanceOf[LeaderAndIsrResponse]
          handleLeaderAndIsrResponse(leaderAndIsrResponse, broker)
        })
      }
    }
    leaderAndIsrRequestMap.clear()
  }

  def handleLeaderAndIsrResponse(response: LeaderAndIsrResponse, broker: Int): Unit

  private def sendUpdateMetadataRequests(controllerEpoch: Int, stateChangeLog: StateChangeLogger): Unit = {
    stateChangeLog.info(s"Sending UpdateMetadata request to brokers $updateMetadataRequestBrokerSet " +
      s"for ${updateMetadataRequestPartitionInfoMap.size} partitions")

    val partitionStates = updateMetadataRequestPartitionInfoMap.values.toBuffer
    val metadataVersion = metadataVersionProvider.apply()
    val updateMetadataRequestVersion: Short =
      if (metadataVersion.isAtLeast(IBP_3_4_IV0)) 8
      else if (metadataVersion.isAtLeast(IBP_2_8_IV1)) 7
      else if (metadataVersion.isAtLeast(IBP_2_4_IV1)) 6
      else if (metadataVersion.isAtLeast(IBP_2_2_IV0)) 5
      else if (metadataVersion.isAtLeast(IBP_1_0_IV0)) 4
      else if (metadataVersion.isAtLeast(IBP_0_10_2_IV0)) 3
      else if (metadataVersion.isAtLeast(IBP_0_10_0_IV1)) 2
      else if (metadataVersion.isAtLeast(IBP_0_9_0)) 1
      else 0

    val liveBrokers = metadataInstance.liveOrShuttingDownBrokers.iterator.map { broker =>
      val endpoints = if (updateMetadataRequestVersion == 0) {
        // Version 0 of UpdateMetadataRequest only supports PLAINTEXT
        val securityProtocol = SecurityProtocol.PLAINTEXT
        val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
        val node = broker.node(listenerName)
        Seq(new UpdateMetadataEndpoint()
          .setHost(node.host)
          .setPort(node.port)
          .setSecurityProtocol(securityProtocol.id)
          .setListener(listenerName.value))
      } else {
        broker.endPoints.map { endpoint =>
          new UpdateMetadataEndpoint()
            .setHost(endpoint.host)
            .setPort(endpoint.port)
            .setSecurityProtocol(endpoint.securityProtocol.id)
            .setListener(endpoint.listenerName.value)
        }
      }
      new UpdateMetadataBroker()
        .setId(broker.id)
        .setEndpoints(endpoints.asJava)
        .setRack(broker.rack.orNull)
    }.toBuffer

    updateMetadataRequestBrokerSet.intersect(metadataInstance.liveOrShuttingDownBrokerIds).foreach { broker =>
      val brokerEpoch = metadataInstance.liveBrokerIdAndEpochs(broker)
      val topicIds = partitionStates.map(_.topicName())
        .distinct
        .filter(metadataInstance.topicIds.contains)
        .map(topic => (topic, metadataInstance.topicIds(topic))).toMap
      val updateMetadataRequestBuilder = new UpdateMetadataRequest.Builder(
        updateMetadataRequestVersion,
        controllerId,
        controllerEpoch,
        brokerEpoch,
        partitionStates.asJava,
        liveBrokers.asJava,
        topicIds.asJava,
        kraftController,
        updateType
      )
      sendRequest(broker, updateMetadataRequestBuilder, (r: AbstractResponse) => {
        val updateMetadataResponse = r.asInstanceOf[UpdateMetadataResponse]
        handleUpdateMetadataResponse(updateMetadataResponse, broker)
      })

    }
    updateMetadataRequestBrokerSet.clear()
    updateMetadataRequestPartitionInfoMap.clear()
  }

  def handleUpdateMetadataResponse(response: UpdateMetadataResponse, broker: Int): Unit

  // send StopReplicaRequest to related brokers
  private def sendStopReplicaRequests(controllerEpoch: Int, stateChangeLog: StateChangeLogger): Unit = {
    val traceEnabled = stateChangeLog.isTraceEnabled
    val metadataVersion = metadataVersionProvider.apply()
    val stopReplicaRequestVersion: Short =
      if (metadataVersion.isAtLeast(IBP_3_4_IV0)) 4
      else if (metadataVersion.isAtLeast(IBP_2_6_IV0)) 3
      else if (metadataVersion.isAtLeast(IBP_2_4_IV1)) 2
      else if (metadataVersion.isAtLeast(IBP_2_2_IV0)) 1
      else 0

    // 定义回调函数
    // The callback function that needs to be executed after StopReplicaRequest is executed successfully
    def responseCallback(brokerId: Int, isPartitionDeleted: TopicPartition => Boolean)
                        (response: AbstractResponse): Unit = {
      val stopReplicaResponse = response.asInstanceOf[StopReplicaResponse]
      val partitionErrorsForDeletingTopics = mutable.Map.empty[TopicPartition, Errors]
      stopReplicaResponse.partitionErrors.forEach { pe =>
        val tp = new TopicPartition(pe.topicName, pe.partitionIndex)
        if (metadataInstance.isTopicDeletionInProgress(pe.topicName) &&
            isPartitionDeleted(tp)) {
          partitionErrorsForDeletingTopics += tp -> Errors.forCode(pe.errorCode)
        }
      }
      if (partitionErrorsForDeletingTopics.nonEmpty) {
        // *发送StopReplicaRequest后，收到响应，执行回调
        handleStopReplicaResponse(stopReplicaResponse, brokerId, partitionErrorsForDeletingTopics.toMap)
      }
    }

    stopReplicaRequestMap.forKeyValue { (brokerId, partitionStates) =>
      if (metadataInstance.liveOrShuttingDownBrokerIds.contains(brokerId)) {
        if (traceEnabled)
          partitionStates.forKeyValue { (topicPartition, partitionState) =>
            stateChangeLog.trace(s"Sending StopReplica request $partitionState to " +
              s"broker $brokerId for partition $topicPartition")
          }

        val brokerEpoch = metadataInstance.liveBrokerIdAndEpochs(brokerId)
        if (stopReplicaRequestVersion >= 3) {
          val stopReplicaTopicState = mutable.Map.empty[String, StopReplicaTopicState]
          partitionStates.forKeyValue { (topicPartition, partitionState) =>
            val topicState = stopReplicaTopicState.getOrElseUpdate(topicPartition.topic,
              new StopReplicaTopicState().setTopicName(topicPartition.topic))
            topicState.partitionStates().add(partitionState)
          }

          stateChangeLog.info(s"Sending StopReplica request for ${partitionStates.size} " +
            s"replicas to broker $brokerId")
          val stopReplicaRequestBuilder = new StopReplicaRequest.Builder(
            stopReplicaRequestVersion, controllerId, controllerEpoch, brokerEpoch,
            false, stopReplicaTopicState.values.toBuffer.asJava, kraftController)
          sendRequest(brokerId, stopReplicaRequestBuilder,
            responseCallback(brokerId, tp => partitionStates.get(tp).exists(_.deletePartition)))
        } else {
          var numPartitionStateWithDelete = 0
          var numPartitionStateWithoutDelete = 0
          val topicStatesWithDelete = mutable.Map.empty[String, StopReplicaTopicState]
          val topicStatesWithoutDelete = mutable.Map.empty[String, StopReplicaTopicState]

          partitionStates.forKeyValue { (topicPartition, partitionState) =>
            val topicStates = if (partitionState.deletePartition()) {
              numPartitionStateWithDelete += 1
              topicStatesWithDelete
            } else {
              numPartitionStateWithoutDelete += 1
              topicStatesWithoutDelete
            }
            val topicState = topicStates.getOrElseUpdate(topicPartition.topic,
              new StopReplicaTopicState().setTopicName(topicPartition.topic))
            topicState.partitionStates().add(partitionState)
          }

          if (topicStatesWithDelete.nonEmpty) {
            stateChangeLog.info(s"Sending StopReplica request (delete = true) for " +
              s"$numPartitionStateWithDelete replicas to broker $brokerId")
            val stopReplicaRequestBuilder = new StopReplicaRequest.Builder(
              stopReplicaRequestVersion, controllerId, controllerEpoch, brokerEpoch,
              true, topicStatesWithDelete.values.toBuffer.asJava, kraftController)
            sendRequest(brokerId, stopReplicaRequestBuilder, responseCallback(brokerId, _ => true))
          }

          if (topicStatesWithoutDelete.nonEmpty) {
            stateChangeLog.info(s"Sending StopReplica request (delete = false) for " +
              s"$numPartitionStateWithoutDelete replicas to broker $brokerId")
            val stopReplicaRequestBuilder = new StopReplicaRequest.Builder(
              stopReplicaRequestVersion, controllerId, controllerEpoch, brokerEpoch,
              false, topicStatesWithoutDelete.values.toBuffer.asJava, kraftController)
            // 将“Controller Request”放入 BlockingQueue
            sendRequest(brokerId, stopReplicaRequestBuilder)
          }
        }
      }
    }

    stopReplicaRequestMap.clear()
  }

  def handleStopReplicaResponse(stopReplicaResponse: StopReplicaResponse, brokerId: Int,
                                partitionErrorsForDeletingTopics: Map[TopicPartition, Errors]): Unit

  def sendRequestsToBrokers(controllerEpoch: Int): Unit = {
    try {
      val stateChangeLog = stateChangeLogger.withControllerEpoch(controllerEpoch)
      sendLeaderAndIsrRequest(controllerEpoch, stateChangeLog)
      sendUpdateMetadataRequests(controllerEpoch, stateChangeLog)
      sendStopReplicaRequests(controllerEpoch, stateChangeLog)
      this.updateType = AbstractControlRequest.Type.UNKNOWN
    } catch {
      case e: Throwable =>
        if (leaderAndIsrRequestMap.nonEmpty) {
          error("Haven't been able to send leader and isr requests, current state of " +
            s"the map is $leaderAndIsrRequestMap. Exception message: $e")
        }
        if (updateMetadataRequestBrokerSet.nonEmpty) {
          error(s"Haven't been able to send metadata update requests to brokers $updateMetadataRequestBrokerSet, " +
            s"current state of the partition info is $updateMetadataRequestPartitionInfoMap. Exception message: $e")
        }
        if (stopReplicaRequestMap.nonEmpty) {
          error("Haven't been able to send stop replica requests, current state of " +
            s"the map is $stopReplicaRequestMap. Exception message: $e")
        }
        throw new IllegalStateException(e)
    }
  }
}

case class ControllerBrokerStateInfo(networkClient: NetworkClient,
                                     brokerNode: Node,  // target brokerID
                                     messageQueue: BlockingQueue[QueueItem], // (controller) Request BlockingQueue
                                     requestSendThread: RequestSendThread, // thread to send Request
                                     queueSizeGauge: Gauge[Int],
                                     requestRateAndTimeMetrics: Timer,
                                     reconfigurableChannelBuilder: Option[Reconfigurable])

