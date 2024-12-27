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

import kafka.server.KafkaConfig
import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition

import scala.collection.Set
import scala.collection.mutable

trait DeletionClient {
  def deleteTopic(topic: String, epochZkVersion: Int): Unit
  def deleteTopicDeletions(topics: Seq[String], epochZkVersion: Int): Unit
  def mutePartitionModifications(topic: String): Unit
  def sendMetadataUpdate(partitions: Set[TopicPartition]): Unit
}

class ControllerDeletionClient(controller: KafkaController, zkClient: KafkaZkClient) extends DeletionClient {
  override def deleteTopic(topic: String, epochZkVersion: Int): Unit = {
    // "${TopicsZNode.path} = s"${BrokersZNode.path}/topics"
    // 删除zk上 topic-assignment信息节点: /"${TopicsZNode.path}/$topic"
    zkClient.deleteTopicZNode(topic, epochZkVersion)
    // 删除zk上 topic的config配置节点： s"${ConfigEntityTypeZNode.path(entityType)}/$entityName"
    zkClient.deleteTopicConfigs(Seq(topic), epochZkVersion)
    // 删除zk上：标志topic删除时写入的节点：s"${AdminZNode.path}/delete_topics/$topic"
    zkClient.deleteTopicDeletions(Seq(topic), epochZkVersion)
  }

  override def deleteTopicDeletions(topics: Seq[String], epochZkVersion: Int): Unit = {
    zkClient.deleteTopicDeletions(topics, epochZkVersion)
  }

  override def mutePartitionModifications(topic: String): Unit = {
    controller.unregisterPartitionModificationsHandlers(Seq(topic))
  }

  override def sendMetadataUpdate(partitions: Set[TopicPartition]): Unit = {
    controller.sendUpdateMetadataRequest(controller.controllerContext.liveOrShuttingDownBrokerIds.toSeq, partitions)
  }
}

/**
 * This manages the state machine for topic deletion.
 * 1. TopicCommand issues topic deletion by creating a new admin path /admin/delete_topics/<topic>
 * 2. The controller listens for child changes on /admin/delete_topic and starts topic deletion for the respective topics
 * 3. The controller's ControllerEventThread handles topic deletion. A topic will be ineligible
 *    for deletion in the following scenarios -
  *   3.1 broker hosting one of the replicas for that topic goes down
  *   3.2 partition reassignment for partitions of that topic is in progress
 * 4. Topic deletion is resumed when -
 *    4.1 broker hosting one of the replicas for that topic is started
 *    4.2 partition reassignment for partitions of that topic completes
 * 5. Every replica for a topic being deleted is in either of the 3 states -
 *    5.1 TopicDeletionStarted Replica enters TopicDeletionStarted phase when onPartitionDeletion is invoked.
 *        This happens when the child change watch for /admin/delete_topics fires on the controller. As part of this state
 *        change, the controller sends StopReplicaRequests to all replicas. It registers a callback for the
 *        StopReplicaResponse when deletePartition=true thereby invoking a callback when a response for delete replica
 *        is received from every replica)
 *    5.2 TopicDeletionSuccessful moves replicas from
 *        TopicDeletionStarted->TopicDeletionSuccessful depending on the error codes in StopReplicaResponse
 *    5.3 TopicDeletionFailed moves replicas from
 *        TopicDeletionStarted->TopicDeletionFailed depending on the error codes in StopReplicaResponse.
 *        In general, if a broker dies and if it hosted replicas for topics being deleted, the controller marks the
 *        respective replicas in TopicDeletionFailed state in the onBrokerFailure callback. The reason is that if a
 *        broker fails before the request is sent and after the replica is in TopicDeletionStarted state,
 *        it is possible that the replica will mistakenly remain in TopicDeletionStarted state and topic deletion
 *        will not be retried when the broker comes back up.
 * 6. A topic is marked successfully deleted only if all replicas are in TopicDeletionSuccessful
 *    state. Topic deletion teardown mode deletes all topic state from the controllerContext
 *    as well as from zookeeper. This is the only time the /brokers/topics/<topic> path gets deleted. On the other hand,
 *    if no replica is in TopicDeletionStarted state and at least one replica is in TopicDeletionFailed state, then
 *    it marks the topic for deletion retry.
 *
 *
 * 5. 正在删除的主题的每个副本都处于以下三种状态之一：
 *      5.1 当调用 onPartitionDeletion 时，副本进入 【TopicDeletionStarted】 阶段。
 *      这发生在controller的zk子节点变更监视器 /admin/delete_topics 触发时。作为这种状态变化的一部分， controller向所有副本发送 StopReplicaRequests。
 *      它为 StopReplicaResponse 注册了一个回调，当 deletePartition=true 时， 因此在收到每个副本的删除副本响应时会调用一个回调。
 *
 *      5.2 TopicDeletionSuccessful 根据 StopReplicaResponse ，将副本从 【TopicDeletionStarted】 转换为 【TopicDeletionSuccessful】
 *
 *      5.3 TopicDeletionFailed 根据 StopReplicaResponse，将副本从 【TopicDeletionStarted】 转换为 【TopicDeletionSuccessful】
 *      一般来说，如果broker死亡，并且它托管了正在删除的主题的副本， Controller会在 onBrokerFailure 回调中将相应的副本标记为 TopicDeletionFailed 状态。
 *      原因是如果broker在请求发送之前失败， 并且在副本处于 TopicDeletionStarted 状态之后，副本可能会错误地保持在 TopicDeletionStarted 状态，并且当broker重新启动时， 主题删除将不会被重试。
 *
 * 6. 只有当所有副本都处于 TopicDeletionSuccessful 状态时，主题才被标记为成功删除。主题删除拆除模式从 controllerContext 以及从 zookeeper 中删除所有主题状态。
 *    这是 /brokers/topics/<topic> 路径被删除的唯一时间。
 *    另一方面， 如果没有副本处于 TopicDeletionStarted 状态，并且至少有一个副本处于 TopicDeletionFailed 状态，那么它将标记主题进行删除重试。
 * @param controller
 */
class TopicDeletionManager(config: KafkaConfig,
                           controllerContext: ControllerContext, // controller视角维护的集群元数据
                           replicaStateMachine: ReplicaStateMachine,
                           partitionStateMachine: PartitionStateMachine,
                           client: DeletionClient // 操作zk节点的client
                          ) extends Logging {
  this.logIdent = s"[Topic Deletion Manager ${config.brokerId}] "
  val isDeleteTopicEnabled: Boolean = config.deleteTopicEnable

  def init(initialTopicsToBeDeleted: Set[String], initialTopicsIneligibleForDeletion: Set[String]): Unit = {
    info(s"Initializing manager with initial deletions: $initialTopicsToBeDeleted, " +
      s"initial ineligible deletions: $initialTopicsIneligibleForDeletion")

    if (isDeleteTopicEnabled) {
      controllerContext.queueTopicDeletion(initialTopicsToBeDeleted)
      controllerContext.topicsIneligibleForDeletion ++= initialTopicsIneligibleForDeletion & controllerContext.topicsToBeDeleted
    } else {
      // if delete topic is disabled clean the topic entries under /admin/delete_topics
      info(s"Removing $initialTopicsToBeDeleted since delete topic is disabled")
      client.deleteTopicDeletions(initialTopicsToBeDeleted.toSeq, controllerContext.epochZkVersion)
    }
  }

  def tryTopicDeletion(): Unit = {
    if (isDeleteTopicEnabled) {
      resumeDeletions()
    }
  }

  /**
   * Invoked by the child change listener on /admin/delete_topics to queue up the topics for deletion. The topic gets added
   * to the topicsToBeDeleted list and only gets removed from the list when the topic deletion has completed successfully
   * i.e. all replicas of all partitions of that topic are deleted successfully.
   * @param topics Topics that should be deleted
   */
  def enqueueTopicsForDeletion(topics: Set[String]): Unit = {
    if (isDeleteTopicEnabled) {
      // add topics to "topicsToBeDeleted" queue
      controllerContext.queueTopicDeletion(topics)
      // resume topic delete operation
      resumeDeletions()
    }
  }

  /**
   * Invoked when any event that can possibly resume topic deletion occurs. These events include -
   * 1. New broker starts up. Any replicas belonging to topics queued up for deletion can be deleted since the broker is up
   * 2. Partition reassignment completes. Any partitions belonging to topics queued up for deletion finished reassignment
   * @param topics Topics for which deletion can be resumed
   */
  def resumeDeletionForTopics(topics: Set[String] = Set.empty): Unit = {
    if (isDeleteTopicEnabled) {
      val topicsToResumeDeletion = topics & controllerContext.topicsToBeDeleted
      if (topicsToResumeDeletion.nonEmpty) {
        controllerContext.topicsIneligibleForDeletion --= topicsToResumeDeletion
        resumeDeletions()
      }
    }
  }

  /**
   * Invoked when a broker that hosts replicas for topics to be deleted goes down. Also invoked when the callback for
   * StopReplicaResponse receives an error code for the replicas of a topic to be deleted. As part of this, the replicas
   * are moved from ReplicaDeletionStarted to ReplicaDeletionIneligible state. Also, the topic is added to the list of topics
   * ineligible for deletion until further notice.
   * @param replicas Replicas for which deletion has failed
   */
  def failReplicaDeletion(replicas: Set[PartitionAndReplica]): Unit = {
    if (isDeleteTopicEnabled) {
      // get "FailedToDelete Replicas" of "to be deleted" topics
      val replicasThatFailedToDelete = replicas.filter(r => isTopicQueuedUpForDeletion(r.topic))
      if (replicasThatFailedToDelete.nonEmpty) {
        // Record the corresponding topic and change the status of the replica
        val topics = replicasThatFailedToDelete.map(_.topic)
        debug(s"Deletion failed for replicas ${replicasThatFailedToDelete.mkString(",")}. Halting deletion for topics $topics")
        replicaStateMachine.handleStateChanges(replicasThatFailedToDelete.toSeq, ReplicaDeletionIneligible)
        markTopicIneligibleForDeletion(topics, reason = "replica deletion failure")
        // restarting the topic deletion action
        resumeDeletions()
      }
    }
  }

  /**
   * Halt delete topic if -
   * 1. replicas being down
   * 2. partition reassignment in progress for some partitions of the topic
   * @param topics Topics that should be marked ineligible for deletion. No op if the topic is was not previously queued up for deletion
   */
  def markTopicIneligibleForDeletion(topics: Set[String], reason: => String): Unit = {
    if (isDeleteTopicEnabled) {
      val newTopicsToHaltDeletion = controllerContext.topicsToBeDeleted & topics
      controllerContext.topicsIneligibleForDeletion ++= newTopicsToHaltDeletion
      if (newTopicsToHaltDeletion.nonEmpty)
        info(s"Halted deletion of topics ${newTopicsToHaltDeletion.mkString(",")} due to $reason")
    }
  }

  private def isTopicIneligibleForDeletion(topic: String): Boolean = {
    if (isDeleteTopicEnabled) {
      controllerContext.topicsIneligibleForDeletion.contains(topic)
    } else
      true
  }

  private def isTopicDeletionInProgress(topic: String): Boolean = {
    if (isDeleteTopicEnabled) {
      controllerContext.isAnyReplicaInState(topic, ReplicaDeletionStarted)
    } else
      false
  }

  def isTopicQueuedUpForDeletion(topic: String): Boolean = {
    if (isDeleteTopicEnabled) {
      controllerContext.isTopicQueuedUpForDeletion(topic)
    } else
      false
  }

  /**
   * Invoked by the StopReplicaResponse callback when it receives no error code for a replica of a topic to be deleted.
   * As part of this, the replicas are moved from ReplicaDeletionStarted to ReplicaDeletionSuccessful state. Tears down
   * the topic if all replicas of a topic have been successfully deleted
   * @param replicas Replicas that were successfully deleted by the broker
   */
  def completeReplicaDeletion(replicas: Set[PartitionAndReplica]): Unit = {
    // get successfully Deleted Replicas
    val successfullyDeletedReplicas = replicas.filter(r => isTopicQueuedUpForDeletion(r.topic))
    debug(s"Deletion successfully completed for replicas ${successfullyDeletedReplicas.mkString(",")}")
    // Change the status of these replicas
    // ***** 更改该副本状态为：ReplicaDeletionSuccessful
    replicaStateMachine.handleStateChanges(successfullyDeletedReplicas.toSeq, ReplicaDeletionSuccessful)
    // restarting the topic deletion action
    // resumeDeletions会判断某个topic的是不是所有的replica状态都是ReplicaDeletionSuccessful，来判断是不是删除完成了
    resumeDeletions()
  }

  /**
   * Topic deletion can be retried if -
   * 1. Topic deletion is not already complete
   * 2. Topic deletion is currently not in progress for that topic
   * 3. Topic is currently marked ineligible for deletion
   * @param topic Topic
   * @return Whether or not deletion can be retried for the topic
   */
  private def isTopicEligibleForDeletion(topic: String): Boolean = {
    controllerContext.isTopicQueuedUpForDeletion(topic) &&
      !isTopicDeletionInProgress(topic) &&
      !isTopicIneligibleForDeletion(topic)
  }

  /**
   * If the topic is queued for deletion but deletion is not currently under progress, then deletion is retried for that topic
   * To ensure a successful retry, reset states for respective replicas from ReplicaDeletionIneligible to OfflineReplica state
   * @param topics Topics for which deletion should be retried
   */
  private def retryDeletionForIneligibleReplicas(topics: Set[String]): Unit = {
    // reset replica states from ReplicaDeletionIneligible to OfflineReplica
    val failedReplicas = topics.flatMap(controllerContext.replicasInState(_, ReplicaDeletionIneligible))
    debug(s"Retrying deletion of topics ${topics.mkString(",")} since replicas ${failedReplicas.mkString(",")} were not successfully deleted")
    replicaStateMachine.handleStateChanges(failedReplicas.toSeq, OfflineReplica)
  }

  private def completeDeleteTopic(topic: String): Unit = {
    // 1. 先撤销关于该topic的zk节点的变更监听
    // deregister partition change listener on the deleted topic. This is to prevent the partition change listener
    // firing before the new topic listener when a deleted topic gets auto created
    client.mutePartitionModifications(topic)

    // 2. get all "ReplicaDeletionSuccessful state" Replica of this topic
    val replicasForDeletedTopic = controllerContext.replicasInState(topic, ReplicaDeletionSuccessful)

    // 3. controller will remove this replica from the state machine as well as its partition assignment cache
    // Replica状态转换为【NonExistentReplica】
    replicaStateMachine.handleStateChanges(replicasForDeletedTopic.toSeq, NonExistentReplica)

    // 4. delete the data of this topic in zk： 删除zk上相关的3个节点
    client.deleteTopic(topic, controllerContext.epochZkVersion)

    // 5. delete the data of this topic in controllerContext： controller维护的元数据中删除该topic
    controllerContext.removeTopic(topic)
  }

  /**
   * Invoked with the list of topics to be deleted
   * It invokes onPartitionDeletion for all partitions of a topic.
   * The updateMetadataRequest is also going to set the leader for the topics being deleted to
   * {@link LeaderAndIsr#LeaderDuringDelete}. This lets each broker know that this topic is being deleted and can be
   * removed from their caches.
   */
  private def onTopicDeletion(topics: Set[String]): Unit = {
    // 1. Find the topics that are in the "topic list to be deleted" but have not yet do deleted.
    val unseenTopicsForDeletion = topics.diff(controllerContext.topicsWithDeletionStarted)
    if (unseenTopicsForDeletion.nonEmpty) {
      // 2. find all partitions of "unseenTopicsForDeletion"
      val unseenPartitionsForDeletion = unseenTopicsForDeletion.flatMap(controllerContext.partitionsForTopic)

      // 3. change these partitions state: [ -> OfflinePartition -> NonExistentPartition ]
      // 调用partitionStateMachine，把待删除的topic的partition状态切换为OfflinePartition状态
      partitionStateMachine.handleStateChanges(unseenPartitionsForDeletion.toSeq, OfflinePartition)
      // 调用partitionStateMachine，把待删除的topic的partition状态切换为NonExistentPartition状态
      partitionStateMachine.handleStateChanges(unseenPartitionsForDeletion.toSeq, NonExistentPartition)

      // 4. adding of unseenTopicsForDeletion to topics with deletion started must be done after the partition
      // state changes to make sure the offlinePartitionCount metric is properly updated
      // 把待删除的topic加入controller维护的topicsWithDeletionStarted中
      controllerContext.beginTopicDeletion(unseenTopicsForDeletion)
    }

    // 5. send update metadata so that brokers "stop serving data" for topics to be deleted
    // aim to tell brokers, Stop crunching data for these topics
    // 给集群中所有broker发送MetadataUpdateRequest
    client.sendMetadataUpdate(topics.flatMap(controllerContext.partitionsForTopic))

    // 6. Invoked by onTopicDeletion with the list of partitions for topics to be deleted, truly deletion of underlying disk files
    // 利用replicaStateMachine能力来在状态转换中真正执行删除
    onPartitionDeletion(topics)
  }

  /**
   * Invoked by onTopicDeletion with the list of partitions for topics to be deleted
   * It does the following -
   * 1. Move all dead replicas directly to ReplicaDeletionIneligible state. Also mark the respective topics ineligible
   *    for deletion if some replicas are dead since it won't complete successfully anyway
   * 2. Move all replicas for the partitions to OfflineReplica state. This will send StopReplicaRequest to the replicas
   *    and LeaderAndIsrRequest to the leader with the shrunk ISR. When the leader replica itself is moved to OfflineReplica state,
   *    it will skip sending the LeaderAndIsrRequest since the leader will be updated to -1
   * 3. Move all replicas to ReplicaDeletionStarted state. This will send StopReplicaRequest with deletePartition=true. And
   *    will delete all persistent data from all replicas of the respective partitions
   */
  private def onPartitionDeletion(topicsToBeDeleted: Set[String]): Unit = {
    // 1. get or create key variable
    // store all "Dead" Replicas
    val allDeadReplicas = mutable.ListBuffer.empty[PartitionAndReplica]
    // store all "Deletion Retry" Replicas
    val allReplicasForDeletionRetry = mutable.ListBuffer.empty[PartitionAndReplica]
    // store all "Not suitable for deletion" topics
    val allTopicsIneligibleForDeletion = mutable.Set.empty[String]

    // 2. traverse topicsToBeDeleted
    topicsToBeDeleted.foreach { topic =>
      // 2.1 get alive and dead Replicas
      val (aliveReplicas, deadReplicas) = controllerContext.replicasForTopic(topic).partition { r =>
        controllerContext.isReplicaOnline(r.replica, r.topicPartition)
      }
      // 2.2 get successfully deleted Replicas
      val successfullyDeletedReplicas = controllerContext.replicasInState(topic, ReplicaDeletionSuccessful)
      // 2.3 get "wait for delete" Replicas: [ aliveReplicas - successfullyDeletedReplicas ]
      val replicasForDeletionRetry = aliveReplicas.diff(successfullyDeletedReplicas)

      // 2.4 update allDeadReplicas and allReplicasForDeletionRetry and allTopicsIneligibleForDeletion
      allDeadReplicas ++= deadReplicas
      allReplicasForDeletionRetry ++= replicasForDeletionRetry
      if (deadReplicas.nonEmpty) {
        debug(s"Dead Replicas (${deadReplicas.mkString(",")}) found for topic $topic")
        allTopicsIneligibleForDeletion += topic
      }
    }

    // 3. move dead replicas directly to failed state
    replicaStateMachine.handleStateChanges(allDeadReplicas, ReplicaDeletionIneligible)

    // 4. send stop replica to all followers that are not in the OfflineReplica state so they stop sending fetch requests to the leader
    // state changes: xxx -> OfflineReplica -> ReplicaDeletionStarted
    // StopReplicaRequest | deletePartition = false: "stop fetch from leader"
    // 把该topic的replica转换为OfflineReplica状态
    replicaStateMachine.handleStateChanges(allReplicasForDeletionRetry, OfflineReplica)

    // ** will send "StopReplicaRequest" to brokers
    // StopReplicaRequest | deletePartition = true: "del from disk"
    // 把该topic的replica转换为ReplicaDeletionStarted状态
    replicaStateMachine.handleStateChanges(allReplicasForDeletionRetry, ReplicaDeletionStarted)

    // 5. have "Not suitable for deletion" topics, Explain why deletion is inappropriate
    if (allTopicsIneligibleForDeletion.nonEmpty) {
      markTopicIneligibleForDeletion(allTopicsIneligibleForDeletion, reason = "offline replicas")
    }
  }

  /**
   * Why is there a situation of "restarting the topic deletion action"?
   * Because in some cases, the topic deletion operation cannot be completed. like "the replicas of Partition are being reallocated"
   * When the operation is completed, the deletion operation can be performed again
   */
  private def resumeDeletions(): Unit = {
    // 1. get or create key variables
    // topicsToBeDeleted get from controllerContext
    val topicsQueuedForDeletion = Set.empty[String] ++ controllerContext.topicsToBeDeleted
    // empty set for "Retry delete topics"
    val topicsEligibleForRetry = mutable.Set.empty[String]
    // empty set for "Wait delete topics"
    val topicsEligibleForDeletion = mutable.Set.empty[String]

    if (topicsQueuedForDeletion.nonEmpty)
      info(s"Handling deletion for topics ${topicsQueuedForDeletion.mkString(",")}")

    // 2. traverse topicsQueuedForDeletion
    topicsQueuedForDeletion.foreach { topic =>
      // 2.1 if all replicas are marked as deleted successfully, then topic deletion is done
        // 该Topic所有的Replica的状态均为：ReplicaDeletionSuccessful
      if (controllerContext.areAllReplicasInState(topic, ReplicaDeletionSuccessful)) {
        // * clear up all state for this topic from controller cache and zookeeper
        completeDeleteTopic(topic)
        info(s"Deletion of topic $topic successfully completed")
      // 2.2
      } else if (!controllerContext.isAnyReplicaInState(topic, ReplicaDeletionStarted)) {
        // if you come here, then no replica is in TopicDeletionStarted and all replicas are not in
        // TopicDeletionSuccessful. That means, that either given topic haven't initiated deletion
        // or there is at least one failed replica (which means topic deletion should be retried).
        if (controllerContext.isAnyReplicaInState(topic, ReplicaDeletionIneligible)) {
          // add topic to "Retry delete topics set"
          topicsEligibleForRetry += topic
        }
      }

      // 2.3 Add topic to the eligible set if it is eligible for deletion.
      if (isTopicEligibleForDeletion(topic)) {
        info(s"Deletion of topic $topic (re)started")
        // add topic to "Wait delete topics set"
        topicsEligibleForDeletion += topic
      }
    }

    // 3. topic deletion retry will be kicked off
    if (topicsEligibleForRetry.nonEmpty) {
      retryDeletionForIneligibleReplicas(topicsEligibleForRetry)
    }

    // 4. topic deletion will be kicked off
    if (topicsEligibleForDeletion.nonEmpty) {
      // truly do delete
      onTopicDeletion(topicsEligibleForDeletion)
    }
  }
}
