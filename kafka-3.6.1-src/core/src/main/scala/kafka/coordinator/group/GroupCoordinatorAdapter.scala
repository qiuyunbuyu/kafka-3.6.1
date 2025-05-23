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
package kafka.coordinator.group

import kafka.common.OffsetAndMetadata
import kafka.server.{KafkaConfig, ReplicaManager, RequestLocal}
import kafka.utils.Implicits.MapExtensionMethods
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.message.{ConsumerGroupHeartbeatRequestData, ConsumerGroupHeartbeatResponseData, DeleteGroupsResponseData, DescribeGroupsResponseData, HeartbeatRequestData, HeartbeatResponseData, JoinGroupRequestData, JoinGroupResponseData, LeaveGroupRequestData, LeaveGroupResponseData, ListGroupsRequestData, ListGroupsResponseData, OffsetCommitRequestData, OffsetCommitResponseData, OffsetDeleteRequestData, OffsetDeleteResponseData, OffsetFetchRequestData, OffsetFetchResponseData, SyncGroupRequestData, SyncGroupResponseData, TxnOffsetCommitRequestData, TxnOffsetCommitResponseData}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.{OffsetCommitRequest, RequestContext, TransactionResult}
import org.apache.kafka.common.utils.{BufferSupplier, Time}
import org.apache.kafka.image.{MetadataDelta, MetadataImage}
import org.apache.kafka.server.util.FutureUtils

import java.util
import java.util.{Optional, OptionalInt, Properties}
import java.util.concurrent.CompletableFuture
import java.util.function.IntSupplier
import scala.collection.{immutable, mutable}
import scala.jdk.CollectionConverters._

object GroupCoordinatorAdapter {
  def apply(
    config: KafkaConfig,
    replicaManager: ReplicaManager,
    time: Time,
    metrics: Metrics
  ): GroupCoordinatorAdapter = {
    new GroupCoordinatorAdapter(
      GroupCoordinator(
        config,
        replicaManager,
        time,
        metrics
      ),
      time
    )
  }
}

/**
 * GroupCoordinatorAdapter is a thin wrapper around kafka.coordinator.group.GroupCoordinator
 * that exposes the new org.apache.kafka.coordinator.group.GroupCoordinator interface.
 */
private[group] class GroupCoordinatorAdapter(
  private val coordinator: GroupCoordinator,
  private val time: Time
) extends org.apache.kafka.coordinator.group.GroupCoordinator {

  override def consumerGroupHeartbeat(
    context: RequestContext,
    request: ConsumerGroupHeartbeatRequestData
  ): CompletableFuture[ConsumerGroupHeartbeatResponseData] = {
    FutureUtils.failedFuture(Errors.UNSUPPORTED_VERSION.exception(
      s"The old group coordinator does not support ${ApiKeys.CONSUMER_GROUP_HEARTBEAT.name} API."
    ))
  }

  override def joinGroup(
    context: RequestContext,
    request: JoinGroupRequestData,
    bufferSupplier: BufferSupplier
  ): CompletableFuture[JoinGroupResponseData] = {
    val future = new CompletableFuture[JoinGroupResponseData]()

    def callback(joinResult: JoinGroupResult): Unit = {
      future.complete(new JoinGroupResponseData()
        .setErrorCode(joinResult.error.code)
        .setGenerationId(joinResult.generationId)
        .setProtocolType(joinResult.protocolType.orNull)
        .setProtocolName(joinResult.protocolName.orNull)
        .setLeader(joinResult.leaderId)
        .setSkipAssignment(joinResult.skipAssignment)
        .setMemberId(joinResult.memberId)
        .setMembers(joinResult.members.asJava)
      )
    }
    // 静态消费者标记
    val groupInstanceId = Option(request.groupInstanceId)

    // Only return MEMBER_ID_REQUIRED error if joinGroupRequest version is >= 4
    // and groupInstanceId is configured to unknown.
    // groupInstanceId是空的，就一定需要MemberId了
    val requireKnownMemberId = context.apiVersion >= 4 && groupInstanceId.isEmpty

    // 解析出(protocol.name, protocol.metadata)
    val protocols = request.protocols.valuesList.asScala.map { protocol =>
      (protocol.name, protocol.metadata)
    }.toList

    val supportSkippingAssignment = context.apiVersion >= 9

    // **
    coordinator.handleJoinGroup(
      request.groupId,
      request.memberId,
      groupInstanceId,
      requireKnownMemberId,
      supportSkippingAssignment,
      context.clientId,
      context.clientAddress.toString,
      request.rebalanceTimeoutMs,
      request.sessionTimeoutMs,
      request.protocolType,
      protocols,
      callback,
      Option(request.reason),
      RequestLocal(bufferSupplier)
    )

    future
  }

  override def syncGroup(
    context: RequestContext,
    request: SyncGroupRequestData,
    bufferSupplier: BufferSupplier
  ): CompletableFuture[SyncGroupResponseData] = {
    val future = new CompletableFuture[SyncGroupResponseData]()
    // 1. 定义回调
    def callback(syncGroupResult: SyncGroupResult): Unit = {
      // SyncGroupResponse构建
      future.complete(new SyncGroupResponseData()
        .setErrorCode(syncGroupResult.error.code)
        .setProtocolType(syncGroupResult.protocolType.orNull)
        .setProtocolName(syncGroupResult.protocolName.orNull)
        .setAssignment(syncGroupResult.memberAssignment)
      )
    }
    // 2. 从SyncGroupResponseData中拆解出 Leader Consumer Member 制订的 assignment
    // [assignment.memberId -> assignment.assignment]
    val assignmentMap = immutable.Map.newBuilder[String, Array[Byte]]
    request.assignments.forEach { assignment =>
      assignmentMap += assignment.memberId -> assignment.assignment
    }

    // 3. 调用coordinator 能力
    coordinator.handleSyncGroup(
      request.groupId,
      request.generationId,
      request.memberId,
      Option(request.protocolType),
      Option(request.protocolName),
      Option(request.groupInstanceId),
      assignmentMap.result(),
      callback,
      RequestLocal(bufferSupplier)
    )

    future
  }

  override def heartbeat(
    context: RequestContext,
    request: HeartbeatRequestData
  ): CompletableFuture[HeartbeatResponseData] = {
    val future = new CompletableFuture[HeartbeatResponseData]()

    coordinator.handleHeartbeat(
      request.groupId,
      request.memberId,
      Option(request.groupInstanceId),
      request.generationId,
      error => future.complete(new HeartbeatResponseData()
        .setErrorCode(error.code))
    )

    future
  }

  override def leaveGroup(
    context: RequestContext,
    request: LeaveGroupRequestData
  ): CompletableFuture[LeaveGroupResponseData] = {
    // future + callback模式
    val future = new CompletableFuture[LeaveGroupResponseData]()
    def callback(leaveGroupResult: LeaveGroupResult): Unit = {
      future.complete(new LeaveGroupResponseData()
        .setErrorCode(leaveGroupResult.topLevelError.code)
        .setMembers(leaveGroupResult.memberResponses.map { member =>
          new LeaveGroupResponseData.MemberResponse()
            .setErrorCode(member.error.code)
            .setMemberId(member.memberId)
            .setGroupInstanceId(member.groupInstanceId.orNull)
        }.asJava)
      )
    }
    // 调coordinator能力
    coordinator.handleLeaveGroup(
      request.groupId,
      request.members.asScala.toList,
      callback
    )

    future
  }

  override def listGroups(
    context: RequestContext,
    request: ListGroupsRequestData
  ): CompletableFuture[ListGroupsResponseData] = {
    // Handle a null array the same as empty
    // 1. 从groupMetadataCache取出对应的List[GroupOverview]
    val (error, groups) = coordinator.handleListGroups(
      Option(request.statesFilter).map(_.asScala.toSet).getOrElse(Set.empty)
    )

    // 2. 遍历List[GroupOverview]，填充 ListGroupsResponse
    val response = new ListGroupsResponseData()
      .setErrorCode(error.code)
    groups.foreach { group =>
      response.groups.add(new ListGroupsResponseData.ListedGroup()
        .setGroupId(group.groupId)
        .setProtocolType(group.protocolType)
        .setGroupState(group.state))
    }

    CompletableFuture.completedFuture(response)
  }

  override def describeGroups(
    context: RequestContext,
    groupIds: util.List[String]
  ): CompletableFuture[util.List[DescribeGroupsResponseData.DescribedGroup]] = {

    def describeGroup(groupId: String): DescribeGroupsResponseData.DescribedGroup = {
      // 1.
      // 其实就是先groupMetadataCache.get(groupId)获取GroupMetadata
      // 然后GroupMetadata.summary 来获取GroupSummary
      val (error, summary) = coordinator.handleDescribeGroup(groupId)

      // 2. 根据第一步获取的GroupSummary，封装返回DescribeGroupsResponse
      new DescribeGroupsResponseData.DescribedGroup()
        .setErrorCode(error.code)
        .setGroupId(groupId)
        .setGroupState(summary.state)
        .setProtocolType(summary.protocolType)
        .setProtocolData(summary.protocol)
        .setMembers(summary.members.map { member =>
          new DescribeGroupsResponseData.DescribedGroupMember()
            .setMemberId(member.memberId)
            .setGroupInstanceId(member.groupInstanceId.orNull)
            .setClientId(member.clientId)
            .setClientHost(member.clientHost)
            .setMemberAssignment(member.assignment)
            .setMemberMetadata(member.metadata)
        }.asJava)
    }

    // groupIds列表中每个groupId调用上面的describeGroup函数
    CompletableFuture.completedFuture(groupIds.asScala.map(describeGroup).asJava)
  }

  override def deleteGroups(
    context: RequestContext,
    groupIds: util.List[String],
    bufferSupplier: BufferSupplier
  ): CompletableFuture[DeleteGroupsResponseData.DeletableGroupResultCollection] = {
    val results = new DeleteGroupsResponseData.DeletableGroupResultCollection()
    coordinator.handleDeleteGroups(
      groupIds.asScala.toSet,
      RequestLocal(bufferSupplier)
    ).forKeyValue { (groupId, error) =>
      results.add(new DeleteGroupsResponseData.DeletableGroupResult()
        .setGroupId(groupId)
        .setErrorCode(error.code))
    }
    CompletableFuture.completedFuture(results)
  }

  override def fetchAllOffsets(
    context: RequestContext,
    groupId: String,
    requireStable: Boolean
  ): CompletableFuture[util.List[OffsetFetchResponseData.OffsetFetchResponseTopics]] = {
    handleFetchOffset(
      groupId,
      requireStable,
      None
    )
  }

  override def fetchOffsets(
    context: RequestContext,
    groupId: String,
    topics: util.List[OffsetFetchRequestData.OffsetFetchRequestTopics],
    requireStable: Boolean
  ): CompletableFuture[util.List[OffsetFetchResponseData.OffsetFetchResponseTopics]] = {

    // 构建要获取Offsets的TopicPartition集合
    val topicPartitions = new mutable.ArrayBuffer[TopicPartition]()
    topics.forEach { topic =>
      topic.partitionIndexes.forEach { partition =>
        topicPartitions += new TopicPartition(topic.name, partition)
      }
    }
    // handleFetchOffset
    handleFetchOffset(
      groupId,
      requireStable,
      Some(topicPartitions.toSeq)
    )
  }

  private def handleFetchOffset(
    groupId: String,
    requireStable: Boolean,
    partitions: Option[Seq[TopicPartition]]
  ): CompletableFuture[util.List[OffsetFetchResponseData.OffsetFetchResponseTopics]] = {
    // handleFetchOffsets
    val (error, results) = coordinator.handleFetchOffsets(
      groupId,
      requireStable,
      partitions
    )

    val future = new CompletableFuture[util.List[OffsetFetchResponseData.OffsetFetchResponseTopics]]()
    if (error != Errors.NONE) {
      future.completeExceptionally(error.exception)
    } else {
      val topicsList = new util.ArrayList[OffsetFetchResponseData.OffsetFetchResponseTopics]()
      val topicsMap = new mutable.HashMap[String, OffsetFetchResponseData.OffsetFetchResponseTopics]()

      results.forKeyValue { (tp, offset) =>
        val topic = topicsMap.get(tp.topic) match {
          case Some(topic) =>
            topic

          case None =>
            val topicOffsets = new OffsetFetchResponseData.OffsetFetchResponseTopics().setName(tp.topic)
            topicsMap += tp.topic -> topicOffsets
            topicsList.add(topicOffsets)
            topicOffsets
        }

        topic.partitions.add(new OffsetFetchResponseData.OffsetFetchResponsePartitions()
          .setPartitionIndex(tp.partition)
          .setMetadata(offset.metadata)
          .setCommittedOffset(offset.offset)
          .setCommittedLeaderEpoch(offset.leaderEpoch.orElse(-1))
          .setErrorCode(offset.error.code))
      }

      future.complete(topicsList)
    }

    future
  }

  override def commitOffsets(
    context: RequestContext,
    request: OffsetCommitRequestData,
    bufferSupplier: BufferSupplier
  ): CompletableFuture[OffsetCommitResponseData] = {
    val currentTimeMs = time.milliseconds
    val future = new CompletableFuture[OffsetCommitResponseData]()

    def callback(commitStatus: Map[TopicIdPartition, Errors]): Unit = {
      val response = new OffsetCommitResponseData()
      val byTopics = new mutable.HashMap[String, OffsetCommitResponseData.OffsetCommitResponseTopic]()

      commitStatus.forKeyValue { (tp, error) =>
        val topic = byTopics.get(tp.topic) match {
          case Some(existingTopic) =>
            existingTopic
          case None =>
            val newTopic = new OffsetCommitResponseData.OffsetCommitResponseTopic().setName(tp.topic)
            byTopics += tp.topic -> newTopic
            response.topics.add(newTopic)
            newTopic
        }

        topic.partitions.add(new OffsetCommitResponseData.OffsetCommitResponsePartition()
          .setPartitionIndex(tp.partition)
          .setErrorCode(error.code))
      }

      future.complete(response)
    }

    // "default" expiration timestamp is defined as now + retention. The retention may be overridden
    // in versions from v2 to v4. Otherwise, the retention defined on the broker is used. If an explicit
    // commit timestamp is provided (v1 only), the expiration timestamp is computed based on that.
    val expireTimeMs = request.retentionTimeMs match {
      case OffsetCommitRequest.DEFAULT_RETENTION_TIME => None
      case retentionTimeMs => Some(currentTimeMs + retentionTimeMs)
    }

    val partitions = new mutable.HashMap[TopicIdPartition, OffsetAndMetadata]()
    request.topics.forEach { topic =>
      topic.partitions.forEach { partition =>
        val tp = new TopicIdPartition(Uuid.ZERO_UUID, partition.partitionIndex, topic.name)
        partitions += tp -> createOffsetAndMetadata(
          currentTimeMs,
          partition.committedOffset,
          partition.committedLeaderEpoch,
          partition.committedMetadata,
          partition.commitTimestamp,
          expireTimeMs
        )
      }
    }

    coordinator.handleCommitOffsets(
      request.groupId,
      request.memberId,
      Option(request.groupInstanceId),
      request.generationIdOrMemberEpoch,
      partitions.toMap,
      callback,
      RequestLocal(bufferSupplier)
    )

    future
  }

  override def commitTransactionalOffsets(
    context: RequestContext,
    request: TxnOffsetCommitRequestData,
    bufferSupplier: BufferSupplier
  ): CompletableFuture[TxnOffsetCommitResponseData] = {
    val currentTimeMs = time.milliseconds
    val future = new CompletableFuture[TxnOffsetCommitResponseData]()

    def callback(results: Map[TopicIdPartition, Errors]): Unit = {
      val response = new TxnOffsetCommitResponseData()
      val byTopics = new mutable.HashMap[String, TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic]()

      results.forKeyValue { (tp, error) =>
        val topic = byTopics.get(tp.topic) match {
          case Some(existingTopic) =>
            existingTopic
          case None =>
            val newTopic = new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic().setName(tp.topic)
            byTopics += tp.topic -> newTopic
            response.topics.add(newTopic)
            newTopic
        }

        topic.partitions.add(new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
          .setPartitionIndex(tp.partition)
          .setErrorCode(error.code))
      }

      future.complete(response)
    }

    val partitions = new mutable.HashMap[TopicIdPartition, OffsetAndMetadata]()
    request.topics.forEach { topic =>
      topic.partitions.forEach { partition =>
        val tp = new TopicIdPartition(Uuid.ZERO_UUID, partition.partitionIndex, topic.name)
        partitions += tp -> createOffsetAndMetadata(
          currentTimeMs,
          partition.committedOffset,
          partition.committedLeaderEpoch,
          partition.committedMetadata,
          OffsetCommitRequest.DEFAULT_TIMESTAMP, // means that currentTimeMs is used.
          None
        )
      }
    }

    coordinator.handleTxnCommitOffsets(
      request.groupId,
      request.producerId,
      request.producerEpoch,
      request.memberId,
      Option(request.groupInstanceId),
      request.generationId,
      partitions.toMap,
      callback,
      RequestLocal(bufferSupplier)
    )

    future
  }

  private def createOffsetAndMetadata(
    currentTimeMs: Long,
    offset: Long,
    leaderEpoch: Int,
    metadata: String,
    commitTimestamp: Long,
    expireTimestamp: Option[Long]
  ): OffsetAndMetadata = {
    new OffsetAndMetadata(
      offset = offset,
      leaderEpoch = leaderEpoch match {
        case RecordBatch.NO_PARTITION_LEADER_EPOCH => Optional.empty[Integer]
        case committedLeaderEpoch => Optional.of[Integer](committedLeaderEpoch)
      },
      metadata = metadata match {
        case null => OffsetAndMetadata.NoMetadata
        case metadata => metadata
      },
      commitTimestamp = commitTimestamp match {
        case OffsetCommitRequest.DEFAULT_TIMESTAMP => currentTimeMs
        case customTimestamp => customTimestamp
      },
      expireTimestamp = expireTimestamp
    )
  }

  override def deleteOffsets(
    context: RequestContext,
    request: OffsetDeleteRequestData,
    bufferSupplier: BufferSupplier
  ): CompletableFuture[OffsetDeleteResponseData] = {
    val future = new CompletableFuture[OffsetDeleteResponseData]()

    val partitions = mutable.ArrayBuffer[TopicPartition]()
    request.topics.forEach { topic =>
      topic.partitions.forEach { partition =>
        partitions += new TopicPartition(topic.name, partition.partitionIndex)
      }
    }

    val (groupError, topicPartitionResults) = coordinator.handleDeleteOffsets(
      request.groupId,
      partitions,
      RequestLocal(bufferSupplier)
    )

    if (groupError != Errors.NONE) {
      future.completeExceptionally(groupError.exception)
    } else {
      val response = new OffsetDeleteResponseData()
      topicPartitionResults.forKeyValue { (topicPartition, error) =>
        var topic = response.topics.find(topicPartition.topic)
        if (topic == null) {
          topic = new OffsetDeleteResponseData.OffsetDeleteResponseTopic().setName(topicPartition.topic)
          response.topics.add(topic)
        }
        topic.partitions.add(new OffsetDeleteResponseData.OffsetDeleteResponsePartition()
          .setPartitionIndex(topicPartition.partition)
          .setErrorCode(error.code))
      }

      future.complete(response)
    }

    future
  }

  override def partitionFor(groupId: String): Int = {
    coordinator.partitionFor(groupId)
  }

  override def onTransactionCompleted(
    producerId: Long,
    partitions: java.lang.Iterable[TopicPartition],
    transactionResult: TransactionResult
  ): Unit = {
    coordinator.scheduleHandleTxnCompletion(
      producerId,
      partitions.asScala,
      transactionResult
    )
  }

  override def onPartitionsDeleted(
    topicPartitions: util.List[TopicPartition],
    bufferSupplier: BufferSupplier
  ): Unit = {
    coordinator.handleDeletedPartitions(topicPartitions.asScala, RequestLocal(bufferSupplier))
  }

  override def onElection(
    groupMetadataPartitionIndex: Int,
    groupMetadataPartitionLeaderEpoch: Int
  ): Unit = {
    coordinator.onElection(groupMetadataPartitionIndex, groupMetadataPartitionLeaderEpoch)
  }

  override def onResignation(
    groupMetadataPartitionIndex: Int,
    groupMetadataPartitionLeaderEpoch: OptionalInt
  ): Unit = {
    coordinator.onResignation(groupMetadataPartitionIndex, groupMetadataPartitionLeaderEpoch)
  }

  override def onNewMetadataImage(
    newImage: MetadataImage,
    delta: MetadataDelta
  ): Unit = {
    // The metadata image is not used in the old group coordinator.
  }

  override def groupMetadataTopicConfigs(): Properties = {
    coordinator.offsetsTopicConfigs
  }

  override def startup(groupMetadataTopicPartitionCount: IntSupplier): Unit = {
    coordinator.startup(() => groupMetadataTopicPartitionCount.getAsInt)
  }

  override def shutdown(): Unit = {
    coordinator.shutdown()
  }
}
