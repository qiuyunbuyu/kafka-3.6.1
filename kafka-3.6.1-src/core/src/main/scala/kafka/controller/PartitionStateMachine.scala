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

import kafka.api.LeaderAndIsr
import kafka.common.StateChangeFailedException
import kafka.controller.Election._
import kafka.server.KafkaConfig
import kafka.utils.Implicits._
import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import kafka.zk.TopicPartitionStateZNode
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.kafka.server.common.MetadataVersion.IBP_3_2_IV0
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.Code

import scala.collection.{Map, Seq, mutable}

abstract class PartitionStateMachine(controllerContext: ControllerContext) extends Logging {
  /**
   * Invoked on successful controller election.
   */
  def startup(): Unit = {
    // 1.
    info("Initializing partition state")
    initializePartitionState()
    // 2.
    info("Triggering online partition state changes")
    triggerOnlinePartitionStateChange()
    debug(s"Started partition state machine with initial state -> ${controllerContext.partitionStates}")
  }

  /**
   * Invoked on controller shutdown.
   */
  def shutdown(): Unit = {
    info("Stopped partition state machine")
  }

  /**
   * This API invokes the OnlinePartition state change on all partitions in either the NewPartition or OfflinePartition
   * state. *This is called on a successful controller election and on broker changes*
   */
  def triggerOnlinePartitionStateChange(): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    // 1. get OfflinePartition and NewPartition sets from controllerContext
    val partitions = controllerContext.partitionsInStates(Set(OfflinePartition, NewPartition))
    // 2. try to move all partitions in NewPartition or OfflinePartition state to OnlinePartition
    triggerOnlineStateChangeForPartitions(partitions)
  }

  def triggerOnlinePartitionStateChange(topic: String): Unit = {
    val partitions = controllerContext.partitionsInStates(topic, Set(OfflinePartition, NewPartition))
    triggerOnlineStateChangeForPartitions(partitions)
  }

  private def triggerOnlineStateChangeForPartitions(partitions: collection.Set[TopicPartition]): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    // 1. try to move all partitions in NewPartition or OfflinePartition state to OnlinePartition state except partitions that belong to topics to be deleted
    val partitionsToTrigger = partitions.filter { partition =>
      !controllerContext.isTopicQueuedUpForDeletion(partition.topic)
    }.toSeq

    // 2. Try to change the state of the given partitions to the given targetState: "OnlinePartition State"
    handleStateChanges(partitionsToTrigger, OnlinePartition, Some(OfflinePartitionLeaderElectionStrategy(false)))
    // TODO: If handleStateChanges catches an exception, it is not enough to bail out and log an error.
    // It is important to trigger leader election for those partitions.
  }

  /**
   * Invoked on startup of the partition's state machine to "set the initial state for all existing partitions" in
   * zookeeper
   */
  private def initializePartitionState(): Unit = {
    // traverse all Partitions of all brokers
    for (topicPartition <- controllerContext.allPartitions) {
      // check if leader and isr path exists for partition. If not, then it is in NEW state
      controllerContext.partitionLeadershipInfo(topicPartition) match {
        case Some(currentLeaderIsrAndEpoch) =>
          // else, check if the leader for partition is alive. If yes, it is in Online state, else it is in Offline state
          if (controllerContext.isReplicaOnline(currentLeaderIsrAndEpoch.leaderAndIsr.leader, topicPartition))
          // leader is alive
            controllerContext.putPartitionState(topicPartition, OnlinePartition)
          else {
            // set "OfflinePartition" State
            controllerContext.putPartitionState(topicPartition, OfflinePartition)
          }
        case None =>
          // if can not find PartitionInfo in zk, set "NewPartition" State
          controllerContext.putPartitionState(topicPartition, NewPartition)
      }
    }
  }
  // handle State Changes
  def handleStateChanges(
    partitions: Seq[TopicPartition],
    targetState: PartitionState
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    handleStateChanges(partitions, targetState, None)
  }
  // handle State Changes and select Leader according to the specified strategy
  def handleStateChanges(
    partitions: Seq[TopicPartition],
    targetState: PartitionState,
    leaderElectionStrategy: Option[PartitionLeaderElectionStrategy]
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]]

}

/**
 * This class represents the state machine for partitions. It defines the states that a partition can be in, and
 * transitions to move the partition to another legal state. The different states that a partition can be in are -
 * 1. NonExistentPartition: This state indicates that the partition was either never created or was created and then
 *                          deleted. Valid previous state, if one exists, is OfflinePartition
 * 2. NewPartition        : After creation, the partition is in the NewPartition state. In this state, the partition should have
 *                          replicas assigned to it, but no leader/isr yet. Valid previous states are NonExistentPartition
 * 3. OnlinePartition     : Once a leader is elected for a partition, it is in the OnlinePartition state.
 *                          Valid previous states are NewPartition/OfflinePartition
 * 4. OfflinePartition    : If, after successful leader election, the leader for partition dies, then the partition
 *                          moves to the OfflinePartition state. Valid previous states are NewPartition/OnlinePartition
 */
class ZkPartitionStateMachine(config: KafkaConfig,
                              stateChangeLogger: StateChangeLogger,
                              controllerContext: ControllerContext,
                              zkClient: KafkaZkClient,
                              controllerBrokerRequestBatch: ControllerBrokerRequestBatch)
  extends PartitionStateMachine(controllerContext) {

  private val isLeaderRecoverySupported = config.interBrokerProtocolVersion.isAtLeast(IBP_3_2_IV0)

  private val controllerId = config.brokerId
  this.logIdent = s"[PartitionStateMachine controllerId=$controllerId] "

  /**
   * Try to change the state of the given partitions to the given targetState, using the given
   * partitionLeaderElectionStrategyOpt if a leader election is required.
   * @param partitions The partitions
   * @param targetState The state
   * @param partitionLeaderElectionStrategyOpt The leader election strategy if a leader election is required.
   * @return A map of failed and successful elections when targetState is OnlinePartitions. The keys are the
   *         topic partitions and the corresponding values are either the exception that was thrown or new
   *         leader & ISR.
   */
  override def handleStateChanges(
    partitions: Seq[TopicPartition],
    targetState: PartitionState,
    partitionLeaderElectionStrategyOpt: Option[PartitionLeaderElectionStrategy]
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    if (partitions.nonEmpty) {
      try {
        // 1. clear up controller Broker RequestBatch
        controllerBrokerRequestBatch.newBatch()
        // 2. ***exercises the partition's state machine
        val result = doHandleStateChanges(
          partitions,
          targetState,
          partitionLeaderElectionStrategyOpt
        )
        // 3. controller need send Request to corresponding broker
        controllerBrokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch)
        // 4. return result
        result
      } catch {
        case e: ControllerMovedException =>
          error(s"Controller moved to another broker when moving some partitions to $targetState state", e)
          throw e
        case e: Throwable =>
          error(s"Error while moving some partitions to $targetState state", e)
          partitions.iterator.map(_ -> Left(e)).toMap
      }
    } else {
      Map.empty
    }
  }

  private def partitionState(partition: TopicPartition): PartitionState = {
    controllerContext.partitionState(partition)
  }

  /**
   * This API exercises the partition's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state. Valid state transitions are:
   * NonExistentPartition -> NewPartition:
   * --load assigned replicas from ZK to controller cache
   *
   * NewPartition -> OnlinePartition
   * --assign first live replica as the leader and all live replicas as the isr; write leader and isr to ZK for this partition
   * --send LeaderAndIsr request to every live replica and UpdateMetadata request to every live broker
   *
   * OnlinePartition,OfflinePartition -> OnlinePartition
   * --select new leader and isr for this partition and a set of replicas to receive the LeaderAndIsr request, and write leader and isr to ZK
   * --for this partition, send LeaderAndIsr request to every receiving replica and UpdateMetadata request to every live broker
   *
   * NewPartition,OnlinePartition,OfflinePartition -> OfflinePartition
   * --nothing other than marking partition state as Offline
   *
   * OfflinePartition -> NonExistentPartition
   * --nothing other than marking the partition state as NonExistentPartition
   * @param partitions  The partitions for which the state transition is invoked
   * @param targetState The end state that the partition should be moved to
   * @return A map of failed and successful elections when targetState is OnlinePartitions. The keys are the
   *         topic partitions and the corresponding values are either the exception that was thrown or new
   *         leader & ISR.
   */
  private def doHandleStateChanges(
    partitions: Seq[TopicPartition],
    targetState: PartitionState,
    partitionLeaderElectionStrategyOpt: Option[PartitionLeaderElectionStrategy]
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    // help for Log
    val stateChangeLog = stateChangeLogger.withControllerEpoch(controllerContext.epoch)
    val traceEnabled = stateChangeLog.isTraceEnabled

    // 1. Initializes all partition state as NonExistentPartition
    partitions.foreach(partition => controllerContext.putPartitionStateIfNotExists(partition, NonExistentPartition))
    // 2. get all validPartitions and invalidPartitions
    val (validPartitions, invalidPartitions) = controllerContext.checkValidPartitionStateChange(partitions, targetState)
    invalidPartitions.foreach(partition => logInvalidTransition(partition, targetState))

    targetState match {
      // case1: -> NewPartition
      case NewPartition =>
        // traverse all validPartitions
        validPartitions.foreach { partition =>
          stateChangeLog.info(s"Changed partition $partition state from ${partitionState(partition)} to $targetState with " +
            s"assigned replicas ${controllerContext.partitionReplicaAssignment(partition).mkString(",")}")
          // set Partition State to NewPartition
          controllerContext.putPartitionState(partition, NewPartition)
        }
        Map.empty

      // case2: -> OnlinePartition
      case OnlinePartition =>
        // a. get all uninitialized Partitions(Partitions in NewPartition State)
        val uninitializedPartitions = validPartitions.filter(partition => partitionState(partition) == NewPartition)
        // b. get all "can elect Leader" Partitions(Partitions in OfflinePartition State or OnlinePartition state)
        val partitionsToElectLeader = validPartitions.filter(partition => partitionState(partition) == OfflinePartition || partitionState(partition) == OnlinePartition)

        // c. handle above "uninitializedPartitions"
        if (uninitializedPartitions.nonEmpty) {
          // c.1 try to write "Leader and ISR" to zk Node
          val successfulInitializations = initializeLeaderAndIsrForPartitions(uninitializedPartitions)
          successfulInitializations.foreach { partition =>
            stateChangeLog.info(s"Changed partition $partition from ${partitionState(partition)} to $targetState with state " +
              s"${controllerContext.partitionLeadershipInfo(partition).get.leaderAndIsr}")
            // c.2 transfer partition state to  OnlinePartition
            controllerContext.putPartitionState(partition, OnlinePartition)
          }
        }
        // d. handle above "partitionsToElectLeader"
        if (partitionsToElectLeader.nonEmpty) {
          // d.1 call electLeaderForPartitions to get electionResults
          val electionResults = electLeaderForPartitions(
            partitionsToElectLeader,
            partitionLeaderElectionStrategyOpt.getOrElse(
              throw new IllegalArgumentException("Election strategy is a required field when the target state is OnlinePartition")
            )
          )
          // d.2 traverse all electionResults
          electionResults.foreach {
            // elect Leader success
            case (partition, Right(leaderAndIsr)) =>
              stateChangeLog.info(
                s"Changed partition $partition from ${partitionState(partition)} to $targetState with state $leaderAndIsr"
              )
              // transfer partition state to  OnlinePartition
              controllerContext.putPartitionState(partition, OnlinePartition)
            // elect Leader fail
            case (_, Left(_)) => // Ignore; no need to update partition state on election error
          }
          // return election Results
          electionResults
        } else {
          Map.empty
        }

      // case3: -> OfflinePartition | NonExistentPartition
      case OfflinePartition | NonExistentPartition =>
        validPartitions.foreach { partition =>
          if (traceEnabled)
            stateChangeLog.trace(s"Changed partition $partition state from ${partitionState(partition)} to $targetState")
          controllerContext.putPartitionState(partition, targetState)
        }
        Map.empty
    }
  }

  /**
   * Initialize leader and isr partition state in zookeeper.
   * @param partitions The partitions  that we're trying to initialize.
   * @return The partitions that have been successfully initialized.
   */
  private def initializeLeaderAndIsrForPartitions(partitions: Seq[TopicPartition]): Seq[TopicPartition] = {
    val successfulInitializations = mutable.Buffer.empty[TopicPartition]
    // 1. get all Replicas of Partition
    val replicasPerPartition = partitions.map(partition => partition -> controllerContext.partitionReplicaAssignment(partition))
    // 2. get all live Replicas of Partition
    val liveReplicasPerPartition = replicasPerPartition.map { case (partition, replicas) =>
        val liveReplicasForPartition = replicas.filter(replica => controllerContext.isReplicaOnline(replica, partition))
        partition -> liveReplicasForPartition
    }
    // 3. classify "Seq[TopicPartition]" to two set
    val (partitionsWithoutLiveReplicas, partitionsWithLiveReplicas) = liveReplicasPerPartition.partition { case (_, liveReplicas) => liveReplicas.isEmpty }

    // 4. handle partitionsWithoutLiveReplicas, Log Failed State Change Msg
    partitionsWithoutLiveReplicas.foreach { case (partition, _) =>
      val failMsg = s"Controller $controllerId epoch ${controllerContext.epoch} encountered error during state change of " +
        s"partition $partition from New to Online, assigned replicas are " +
        s"[${controllerContext.partitionReplicaAssignment(partition).mkString(",")}], live brokers are [${controllerContext.liveBrokerIds}]. No assigned " +
        "replica is alive."
      logFailedStateChange(partition, NewPartition, OnlinePartition, new StateChangeFailedException(failMsg))
    }

    // 5. handle partitionsWithLiveReplicas
    val leaderIsrAndControllerEpochs = partitionsWithLiveReplicas.map { case (partition, liveReplicas) =>
      // 5.1 choose Leader
      val leaderAndIsr = LeaderAndIsr(liveReplicas.head, liveReplicas.toList)
      // 5.2 choose ISR
      val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerContext.epoch)
      partition -> leaderIsrAndControllerEpoch
    }.toMap

    // 6. try to write leader and ISR data to zk node
    val createResponses = try {
      zkClient.createTopicPartitionStatesRaw(leaderIsrAndControllerEpochs, controllerContext.epochZkVersion)
    } catch {
      case e: ControllerMovedException =>
        error("Controller moved to another broker when trying to create the topic partition state znode", e)
        throw e
      case e: Exception =>
        // if find Exception, Log Failed info, and add this Partition to "fail sets"
        partitionsWithLiveReplicas.foreach { case (partition, _) => logFailedStateChange(partition, partitionState(partition), NewPartition, e) }
        Seq.empty
    }

    // 6. handle createResponses and "addLeaderAndIsrRequestForBrokers"
    createResponses.foreach { createResponse =>
      val code = createResponse.resultCode
      val partition = createResponse.ctx.get.asInstanceOf[TopicPartition]
      val leaderIsrAndControllerEpoch = leaderIsrAndControllerEpochs(partition)
      if (code == Code.OK) {
        controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)
        // "addLeaderAndIsrRequestForBrokers"
        controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(leaderIsrAndControllerEpoch.leaderAndIsr.isr,
          partition, leaderIsrAndControllerEpoch, controllerContext.partitionFullReplicaAssignment(partition), isNew = true)
        successfulInitializations += partition
      } else {
        logFailedStateChange(partition, NewPartition, OnlinePartition, code)
      }
    }
    // 7. return successful initialize Partitions sets
    successfulInitializations
  }

  /**
   * Repeatedly attempt to elect leaders for multiple partitions until there are no more remaining partitions to retry.
   * @param partitions The partitions that we're trying to elect leaders for.
   * @param partitionLeaderElectionStrategy The election strategy to use.
   * @return A map of failed and successful elections. The keys are the topic partitions and the corresponding values are
   *         either the exception that was thrown or new leader & ISR.
   */
  private def electLeaderForPartitions(
    partitions: Seq[TopicPartition],
    partitionLeaderElectionStrategy: PartitionLeaderElectionStrategy
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {

    // 1. get or create key variables
    var remaining = partitions
    val finishedElections = mutable.Map.empty[TopicPartition, Either[Throwable, LeaderAndIsr]]

    // 2. traverse remaining partitions to elect Leader
    while (remaining.nonEmpty) {
      // 2.1 call doElectLeaderForPartitions to Elect Leader for Partitions, and classify results to (finished, updatesToRetry)
      val (finished, updatesToRetry) = doElectLeaderForPartitions(remaining, partitionLeaderElectionStrategy)

      // 2.2 update "remaining"
      remaining = updatesToRetry

      // 2.3 traverse "finished"
      finished.foreach {
        // if elect Failed -> log
        case (partition, Left(e)) =>
          logFailedStateChange(partition, partitionState(partition), OnlinePartition, e)
        // if elect Success -> do nothing
        case (_, Right(_)) => // Ignore; success so no need to log failed state change
      }

      // 2.4 Cumulative successful partitions
      finishedElections ++= finished

      if (remaining.nonEmpty)
        logger.info(s"Retrying leader election with strategy $partitionLeaderElectionStrategy for partitions $remaining")
    }
    // 3. return A map of failed and successful elections
    finishedElections.toMap
  }

  /**
   * Try to elect leaders for multiple partitions.
   * Electing a leader for a partition updates partition state in zookeeper.
   *
   * @param partitions The partitions that we're trying to elect leaders for.
   * @param partitionLeaderElectionStrategy The election strategy to use.
   * @return A tuple of two values:
   *         1. The partitions and the expected leader and isr that successfully had a leader elected. And exceptions
   *         corresponding to failed elections that should not be retried.
   *         2. The partitions that we should retry due to a zookeeper BADVERSION conflict. Version conflicts can occur if
   *         the partition leader updated partition state while the controller attempted to update partition state.
   */
  private def doElectLeaderForPartitions(
    partitions: Seq[TopicPartition],
    partitionLeaderElectionStrategy: PartitionLeaderElectionStrategy
  ): (Map[TopicPartition, Either[Exception, LeaderAndIsr]], Seq[TopicPartition]) = {

    // get data from "${TopicPartitionZNode.path(partition)}/state" Node in zk
    //    data example:
    //      {"controller_epoch":60,"leader":3,"version":1,"leader_epoch":114,"isr":[2,3]}
    val getDataResponses = try {
      zkClient.getTopicPartitionStatesRaw(partitions)
    } catch {
      case e: Exception =>
        return (partitions.iterator.map(_ -> Left(e)).toMap, Seq.empty)
    }

    // create two map to store [failedElections] and [validLeaderAndIsrs]
    val failedElections = mutable.Map.empty[TopicPartition, Either[Exception, LeaderAndIsr]]
    val validLeaderAndIsrs = mutable.Buffer.empty[(TopicPartition, LeaderAndIsr)]

    // ============================ a. Determine which partitions can elect Leaders ==================
    // traverse getDataResponses
    getDataResponses.foreach { getDataResponse =>
      // 1 get partition from zk
      val partition = getDataResponse.ctx.get.asInstanceOf[TopicPartition]
      // 2 get state of this partition in controllerContext
      val currState = partitionState(partition)
      if (getDataResponse.resultCode == Code.OK) {
        // 3 Parse data from get from zk
        TopicPartitionStateZNode.decode(getDataResponse.data, getDataResponse.stat) match {
          // can get leaderIsrAndControllerEpoch
          case Some(leaderIsrAndControllerEpoch) =>
            // if controller_epoch in zk node > controllerContext.epoch, means this Partition Leader Elect have done by other new controller
            // Terminate this election
            if (leaderIsrAndControllerEpoch.controllerEpoch > controllerContext.epoch) {
              val failMsg = s"Aborted leader election for partition $partition since the LeaderAndIsr path was " +
                s"already written by another controller. This probably means that the current controller $controllerId went through " +
                s"a soft failure and another controller was elected with epoch ${leaderIsrAndControllerEpoch.controllerEpoch}."
              // put this partition to failedElections
              failedElections.put(partition, Left(new StateChangeFailedException(failMsg)))
            } else {
              // put this partition to validLeaderAndIsrs
              validLeaderAndIsrs += partition -> leaderIsrAndControllerEpoch.leaderAndIsr
            }
          // get nothing
          case None =>
            val exception = new StateChangeFailedException(s"LeaderAndIsr information doesn't exist for partition $partition in $currState state")
            failedElections.put(partition, Left(exception))
        }
      // other case, put this partition to failedElections
      } else if (getDataResponse.resultCode == Code.NONODE) {
        val exception = new StateChangeFailedException(s"LeaderAndIsr information doesn't exist for partition $partition in $currState state")
        failedElections.put(partition, Left(exception))
      } else {
        failedElections.put(partition, Left(getDataResponse.resultException.get))
      }
    }
    // nothing Partition can elect Leader , return directly
    if (validLeaderAndIsrs.isEmpty) {
      return (failedElections.toMap, Seq.empty)
    }

    // ============================ b. Start electing Leader ===================================
    val (partitionsWithoutLeaders, partitionsWithLeaders) = partitionLeaderElectionStrategy match {
      // case1: Leader election triggered by Leader Replica offline
      case OfflinePartitionLeaderElectionStrategy(allowUnclean) =>
        val partitionsWithUncleanLeaderElectionState = collectUncleanLeaderElectionState(
          validLeaderAndIsrs,
          allowUnclean
        )
        leaderForOffline(
          controllerContext,
          isLeaderRecoverySupported,
          partitionsWithUncleanLeaderElectionState
        ).partition(_.leaderAndIsr.isEmpty)

      // case2: Leader election triggered by reassign Partition
      case ReassignPartitionLeaderElectionStrategy =>
        leaderForReassign(controllerContext, validLeaderAndIsrs).partition(_.leaderAndIsr.isEmpty)

      // case3: Leader election triggered by Preferred Replica
      case PreferredReplicaPartitionLeaderElectionStrategy =>
        leaderForPreferredReplica(controllerContext, validLeaderAndIsrs).partition(_.leaderAndIsr.isEmpty)

      // case4: Leader election triggered by Controller Shutdown
      case ControlledShutdownPartitionLeaderElectionStrategy =>
        leaderForControlledShutdown(controllerContext, validLeaderAndIsrs).partition(_.leaderAndIsr.isEmpty)
    }

    // ============================ c. update metadata ===================================
    // 1. handle partitionsWithoutLeaders
    partitionsWithoutLeaders.foreach { electionResult =>
      val partition = electionResult.topicPartition
      val failMsg = s"Failed to elect leader for partition $partition under strategy $partitionLeaderElectionStrategy"
      // add this Partition to "fail sets"
      failedElections.put(partition, Left(new StateChangeFailedException(failMsg)))
    }

    // 2. get data from above partitionsWithLeaders
    // AR List
    val recipientsPerPartition = partitionsWithLeaders.map(result => result.topicPartition -> result.liveReplicas).toMap
    // ISR List
    val adjustedLeaderAndIsrs = partitionsWithLeaders.map(result => result.topicPartition -> result.leaderAndIsr.get).toMap

    // 3. use [new Leader] + [new ISR] to update "${TopicPartitionZNode.path(partition)}/state" Node in zk
    val UpdateLeaderAndIsrResult(finishedUpdates, updatesToRetry) = zkClient.updateLeaderAndIsr(
      adjustedLeaderAndIsrs, controllerContext.epoch, controllerContext.epochZkVersion)

    // 4. parse finishedUpdates
    finishedUpdates.forKeyValue { (partition, result) =>
      result.foreach { leaderAndIsr =>
        // 4.1 get replicaAssignment from controllerContext
        val replicaAssignment = controllerContext.partitionFullReplicaAssignment(partition)
        // 4.2 init LeaderAndIsr Request, and add this request to controller "Collection of requests to be sent"
        val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerContext.epoch)
        controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)
        // 4.3 add LeaderAndIsr Request for Brokers
        controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(recipientsPerPartition(partition), partition,
          leaderIsrAndControllerEpoch, replicaAssignment, isNew = false)
      }
    }

    if (isDebugEnabled) {
      updatesToRetry.foreach { partition =>
        debug(s"Controller failed to elect leader for partition $partition. " +
          s"Attempted to write state ${adjustedLeaderAndIsrs(partition)}, but failed with bad ZK version. This will be retried.")
      }
    }
    // ============================ d. return ===================================
    (finishedUpdates ++ failedElections, updatesToRetry)
  }

  /* For the provided set of topic partition and partition sync state it attempts to determine if unclean
   * leader election should be performed. Unclean election should be performed if there are no live
   * replica which are in sync and unclean leader election is allowed (allowUnclean parameter is true or
   * the topic has been configured to allow unclean election).
   *
   * @param leaderIsrAndControllerEpochs set of partition to determine if unclean leader election should be
   *                                     allowed
   * @param allowUnclean whether to allow unclean election without having to read the topic configuration
   * @return a sequence of three element tuple:
   *         1. topic partition
   *         2. leader, isr and controller epoc. Some means election should be performed
   *         3. allow unclean
   */
  private def collectUncleanLeaderElectionState(
    leaderAndIsrs: Seq[(TopicPartition, LeaderAndIsr)],
    allowUnclean: Boolean
  ): Seq[(TopicPartition, Option[LeaderAndIsr], Boolean)] = {
    val (partitionsWithNoLiveInSyncReplicas, partitionsWithLiveInSyncReplicas) = leaderAndIsrs.partition {
      case (partition, leaderAndIsr) =>
        val liveInSyncReplicas = leaderAndIsr.isr.filter(controllerContext.isReplicaOnline(_, partition))
        liveInSyncReplicas.isEmpty
    }

    val electionForPartitionWithoutLiveReplicas = if (allowUnclean) {
      partitionsWithNoLiveInSyncReplicas.map { case (partition, leaderAndIsr) =>
        (partition, Option(leaderAndIsr), true)
      }
    } else {
      val (logConfigs, failed) = zkClient.getLogConfigs(
        partitionsWithNoLiveInSyncReplicas.iterator.map { case (partition, _) => partition.topic }.toSet,
        config.originals()
      )

      partitionsWithNoLiveInSyncReplicas.map { case (partition, leaderAndIsr) =>
        if (failed.contains(partition.topic)) {
          logFailedStateChange(partition, partitionState(partition), OnlinePartition, failed(partition.topic))
          (partition, None, false)
        } else {
          (
            partition,
            Option(leaderAndIsr),
            logConfigs(partition.topic).uncleanLeaderElectionEnable.booleanValue()
          )
        }
      }
    }

    electionForPartitionWithoutLiveReplicas ++
    partitionsWithLiveInSyncReplicas.map { case (partition, leaderAndIsr) =>
      (partition, Option(leaderAndIsr), false)
    }
  }

  private def logInvalidTransition(partition: TopicPartition, targetState: PartitionState): Unit = {
    val currState = partitionState(partition)
    val e = new IllegalStateException(s"Partition $partition should be in one of " +
      s"${targetState.validPreviousStates.mkString(",")} states before moving to $targetState state. Instead it is in " +
      s"$currState state")
    logFailedStateChange(partition, currState, targetState, e)
  }

  private def logFailedStateChange(partition: TopicPartition, currState: PartitionState, targetState: PartitionState, code: Code): Unit = {
    logFailedStateChange(partition, currState, targetState, KeeperException.create(code))
  }

  private def logFailedStateChange(partition: TopicPartition, currState: PartitionState, targetState: PartitionState, t: Throwable): Unit = {
    stateChangeLogger.withControllerEpoch(controllerContext.epoch)
      .error(s"Controller $controllerId epoch ${controllerContext.epoch} failed to change state for partition $partition " +
        s"from $currState to $targetState", t)
  }
}

object PartitionLeaderElectionAlgorithms {
  // Leader Election Strategy1
  def offlinePartitionLeaderElection(assignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int], uncleanLeaderElectionEnabled: Boolean, controllerContext: ControllerContext): Option[Int] = {
    // first [ live and inISR ] Replica
    assignment.find(id => liveReplicas.contains(id) && isr.contains(id)).orElse {
      // if allow unclean Leader Election: only need choose [ live ] Replica (not require in ISR)
      if (uncleanLeaderElectionEnabled) {
        val leaderOpt = assignment.find(liveReplicas.contains)
        if (leaderOpt.isDefined)
          controllerContext.stats.uncleanLeaderElectionRate.mark()
        leaderOpt
      } else {
        None
      }
    }
  }
  // Leader Election Strategy2
  def reassignPartitionLeaderElection(reassignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int]): Option[Int] = {
    // find first [ live and inISR ] Replica in "reassignment"
    reassignment.find(id => liveReplicas.contains(id) && isr.contains(id))
  }
  // Leader Election Strategy3
  def preferredReplicaPartitionLeaderElection(assignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int]): Option[Int] = {
    // find first [ live and inISR ] Replica in "assignment"
    assignment.headOption.filter(id => liveReplicas.contains(id) && isr.contains(id))
  }
  // Leader Election Strategy4
  def controlledShutdownPartitionLeaderElection(assignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int], shuttingDownBrokers: Set[Int]): Option[Int] = {
    // find first [ live and inISR and not in shuttingDownBrokers ] Replica in "assignment"
    assignment.find(id => liveReplicas.contains(id) && isr.contains(id) && !shuttingDownBrokers.contains(id))
  }
}

sealed trait PartitionLeaderElectionStrategy
// Leader election triggered by Leader Replica offline
final case class OfflinePartitionLeaderElectionStrategy(allowUnclean: Boolean) extends PartitionLeaderElectionStrategy
// Leader election triggered by reassign Partition
final case object ReassignPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy
// Leader election triggered by Preferred Replica
final case object PreferredReplicaPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy
// Leader election triggered by Controller Shutdown
final case object ControlledShutdownPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy

sealed trait PartitionState {
  // use Byte to represent state
  def state: Byte
  // define valid Previous States
  def validPreviousStates: Set[PartitionState]
}

case object NewPartition extends PartitionState {
  val state: Byte = 0
  val validPreviousStates: Set[PartitionState] = Set(NonExistentPartition)
}

case object OnlinePartition extends PartitionState {
  val state: Byte = 1
  val validPreviousStates: Set[PartitionState] = Set(NewPartition, OnlinePartition, OfflinePartition)
}

case object OfflinePartition extends PartitionState {
  val state: Byte = 2
  val validPreviousStates: Set[PartitionState] = Set(NewPartition, OnlinePartition, OfflinePartition)
}

case object NonExistentPartition extends PartitionState {
  val state: Byte = 3
  val validPreviousStates: Set[PartitionState] = Set(OfflinePartition)
}
