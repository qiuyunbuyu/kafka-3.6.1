/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.coordinator.transaction

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import kafka.server.{KafkaConfig, MetadataCache, ReplicaManager, RequestLocal}
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnResult
import org.apache.kafka.common.message.{DescribeTransactionsResponseData, ListTransactionsResponseData}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.{AddPartitionsToTxnResponse, TransactionResult}
import org.apache.kafka.common.utils.{LogContext, ProducerIdAndEpoch, Time}
import org.apache.kafka.server.util.Scheduler

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

object TransactionCoordinator {

  def apply(config: KafkaConfig,
            replicaManager: ReplicaManager,
            scheduler: Scheduler,
            createProducerIdGenerator: () => ProducerIdManager,
            metrics: Metrics,
            metadataCache: MetadataCache,
            time: Time): TransactionCoordinator = {

    val txnConfig = TransactionConfig(config.transactionalIdExpirationMs,
      config.transactionMaxTimeoutMs,
      config.transactionTopicPartitions,
      config.transactionTopicReplicationFactor,
      config.transactionTopicSegmentBytes,
      config.transactionsLoadBufferSize,
      config.transactionTopicMinISR,
      config.transactionAbortTimedOutTransactionCleanupIntervalMs,
      config.transactionRemoveExpiredTransactionalIdCleanupIntervalMs,
      config.requestTimeoutMs)

    val txnStateManager = new TransactionStateManager(config.brokerId, scheduler, replicaManager, txnConfig,
      time, metrics)

    val logContext = new LogContext(s"[TransactionCoordinator id=${config.brokerId}] ")
    val txnMarkerChannelManager = TransactionMarkerChannelManager(config, metrics, metadataCache, txnStateManager,
      time, logContext)

    new TransactionCoordinator(txnConfig, scheduler, createProducerIdGenerator, txnStateManager, txnMarkerChannelManager,
      time, logContext)
  }

  private def initTransactionError(error: Errors): InitProducerIdResult = {
    InitProducerIdResult(RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, error)
  }

  private def initTransactionMetadata(txnMetadata: TxnTransitMetadata): InitProducerIdResult = {
    InitProducerIdResult(txnMetadata.producerId, txnMetadata.producerEpoch, Errors.NONE)
  }
}

/**
 * Transaction coordinator handles message transactions sent by producers and communicate with brokers
 * to update ongoing transaction's status.
 *
 * Each Kafka server instantiates a transaction coordinator which is responsible for a set of
 * producers. Producers with specific transactional ids are assigned to their corresponding coordinators;
 * Producers with no specific transactional id may talk to a random broker as their coordinators.
 */
class TransactionCoordinator(txnConfig: TransactionConfig,
                             scheduler: Scheduler,
                             createProducerIdManager: () => ProducerIdManager,
                             txnManager: TransactionStateManager,
                             txnMarkerChannelManager: TransactionMarkerChannelManager,
                             time: Time,
                             logContext: LogContext) extends Logging {
  this.logIdent = logContext.logPrefix

  import TransactionCoordinator._

  type InitProducerIdCallback = InitProducerIdResult => Unit
  type AddPartitionsCallback = Errors => Unit
  type VerifyPartitionsCallback = AddPartitionsToTxnResult => Unit
  type EndTxnCallback = Errors => Unit
  type ApiResult[T] = Either[Errors, T]

  /* Active flag of the coordinator */
  private val isActive = new AtomicBoolean(false)

  val producerIdManager = createProducerIdManager()

  def handleInitProducerId(transactionalId: String,
                           transactionTimeoutMs: Int,
                           expectedProducerIdAndEpoch: Option[ProducerIdAndEpoch],
                           responseCallback: InitProducerIdCallback,
                           requestLocal: RequestLocal = RequestLocal.NoCaching): Unit = {
    // case1: "transactionalId == null" means only "enable.idempotence"
    if (transactionalId == null) {
      // if the transactional id is null, then always blindly accept the request
      // and return a new producerId from the producerId manager
      producerIdManager.generateProducerId() match {
        case Success(producerId) =>
          responseCallback(InitProducerIdResult(producerId, producerEpoch = 0, Errors.NONE))
        case Failure(exception) =>
          responseCallback(initTransactionError(Errors.forException(exception)))
      }
    }
    // case2: Exception INVALID_REQUEST
    else if (transactionalId.isEmpty) {
      // if transactional id is empty then return error as invalid request. This is
      // to make TransactionCoordinator's behavior consistent with producer client
      responseCallback(initTransactionError(Errors.INVALID_REQUEST))
    }
    // case3: Exception INVALID_TRANSACTION_TIMEOUT
    else if (!txnManager.validateTransactionTimeoutMs(transactionTimeoutMs)) {
      // check transactionTimeoutMs is not larger than the broker configured maximum allowed value
      responseCallback(initTransactionError(Errors.INVALID_TRANSACTION_TIMEOUT))
    }
    // case4: a transactional producer
    else {
      // 4.1
      // Get the transaction metadata associated with the given transactional id
      // CoordinatorEpochAndTxnMetadata(coordinatorEpoch: Int, transactionMetadata: TransactionMetadata)
      val coordinatorEpochAndMetadata = txnManager.getTransactionState(transactionalId).flatMap {
        // a. if this transactionalId not have some metadata, will create a new producerId
        case None =>
          producerIdManager.generateProducerId() match {
            case Success(producerId) =>
              // create
              val createdMetadata = new TransactionMetadata(transactionalId = transactionalId,
                producerId = producerId,
                lastProducerId = RecordBatch.NO_PRODUCER_ID,
                producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
                lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
                txnTimeoutMs = transactionTimeoutMs,
                state = Empty,
                topicPartitions = collection.mutable.Set.empty[TopicPartition],
                txnLastUpdateTimestamp = time.milliseconds())
              // put
              txnManager.putTransactionStateIfNotExists(createdMetadata)

            case Failure(exception) =>
              Left(Errors.forException(exception))
          }

        case Some(epochAndTxnMetadata) => Right(epochAndTxnMetadata)
      }

      // 4.2: prepare Init ProducerId Transit
      val result: ApiResult[(Int, TxnTransitMetadata)] = coordinatorEpochAndMetadata.flatMap {
        existingEpochAndMetadata =>
          val coordinatorEpoch = existingEpochAndMetadata.coordinatorEpoch
          val txnMetadata = existingEpochAndMetadata.transactionMetadata

          txnMetadata.inLock {
            prepareInitProducerIdTransit(transactionalId, transactionTimeoutMs, coordinatorEpoch, txnMetadata,
              expectedProducerIdAndEpoch)
          }
      }

      result match {
        case Left(error) =>
          responseCallback(initTransactionError(error))

        case Right((coordinatorEpoch, newMetadata)) =>
          if (newMetadata.txnState == PrepareEpochFence) {
            // abort the ongoing transaction and then return CONCURRENT_TRANSACTIONS to let client wait and retry
            def sendRetriableErrorCallback(error: Errors): Unit = {
              if (error != Errors.NONE) {
                responseCallback(initTransactionError(error))
              } else {
                responseCallback(initTransactionError(Errors.CONCURRENT_TRANSACTIONS))
              }
            }

            endTransaction(transactionalId,
              newMetadata.producerId,
              newMetadata.producerEpoch,
              TransactionResult.ABORT,
              isFromClient = false,
              sendRetriableErrorCallback,
              requestLocal)
          } else {
            def sendPidResponseCallback(error: Errors): Unit = {
              if (error == Errors.NONE) {
                info(s"Initialized transactionalId $transactionalId with producerId ${newMetadata.producerId} and producer " +
                  s"epoch ${newMetadata.producerEpoch} on partition " +
                  s"${Topic.TRANSACTION_STATE_TOPIC_NAME}-${txnManager.partitionFor(transactionalId)}")
                responseCallback(initTransactionMetadata(newMetadata))
              } else {
                info(s"Returning $error error code to client for $transactionalId's InitProducerId request")
                responseCallback(initTransactionError(error))
              }
            }

            txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, newMetadata,
              sendPidResponseCallback, requestLocal = requestLocal)
          }
      }
    }
  }

  private def prepareInitProducerIdTransit(transactionalId: String,
                                           transactionTimeoutMs: Int,
                                           coordinatorEpoch: Int,
                                           txnMetadata: TransactionMetadata,
                                           expectedProducerIdAndEpoch: Option[ProducerIdAndEpoch]): ApiResult[(Int, TxnTransitMetadata)] = {

    def isValidProducerId(producerIdAndEpoch: ProducerIdAndEpoch): Boolean = {
      // If a producer ID and epoch are provided by the request, fence the producer unless one of the following is true:
      //   1. The producer epoch is equal to -1, which implies that the metadata was just created. This is the case of a
      //      producer recovering from an UNKNOWN_PRODUCER_ID error, and it is safe to return the newly-generated
      //      producer ID.
      //   2. The expected producer ID matches the ID in current metadata (the epoch will be checked when we try to
      //      increment it)
      //   3. The expected producer ID matches the previous one and the expected epoch is exhausted, in which case this
      //      could be a retry after a valid epoch bump that the producer never received the response for
      txnMetadata.producerEpoch == RecordBatch.NO_PRODUCER_EPOCH ||
        producerIdAndEpoch.producerId == txnMetadata.producerId ||
        (producerIdAndEpoch.producerId == txnMetadata.lastProducerId && TransactionMetadata.isEpochExhausted(producerIdAndEpoch.epoch))
    }

    if (txnMetadata.pendingTransitionInProgress) {
      // return a retriable exception to let the client backoff and retry
      Left(Errors.CONCURRENT_TRANSACTIONS)
    }
    else if (!expectedProducerIdAndEpoch.forall(isValidProducerId)) {
      Left(Errors.PRODUCER_FENCED)
    } else {
      // caller should have synchronized on txnMetadata already
      txnMetadata.state match {
        case PrepareAbort | PrepareCommit =>
          // reply to client and let it backoff and retry
          Left(Errors.CONCURRENT_TRANSACTIONS)

        case CompleteAbort | CompleteCommit | Empty =>
          val transitMetadataResult =
            // If the epoch is exhausted and the expected epoch (if provided) matches it, generate a new producer ID
            if (txnMetadata.isProducerEpochExhausted &&
                expectedProducerIdAndEpoch.forall(_.epoch == txnMetadata.producerEpoch)) {

              producerIdManager.generateProducerId() match {
                case Success(producerId) =>
                  Right(txnMetadata.prepareProducerIdRotation(producerId, transactionTimeoutMs, time.milliseconds(),
                    expectedProducerIdAndEpoch.isDefined))
                case Failure(exception) =>
                  Left(Errors.forException(exception))
              }
            } else {
              txnMetadata.prepareIncrementProducerEpoch(transactionTimeoutMs, expectedProducerIdAndEpoch.map(_.epoch),
                time.milliseconds())
            }

          transitMetadataResult match {
            case Right(transitMetadata) => Right((coordinatorEpoch, transitMetadata))
            case Left(err) => Left(err)
          }

        case Ongoing =>
          // indicate to abort the current ongoing txn first. Note that this epoch is never returned to the
          // user. We will abort the ongoing transaction and return CONCURRENT_TRANSACTIONS to the client.
          // This forces the client to retry, which will ensure that the epoch is bumped a second time. In
          // particular, if fencing the current producer exhausts the available epochs for the current producerId,
          // then when the client retries, we will generate a new producerId.
          Right(coordinatorEpoch, txnMetadata.prepareFenceProducerEpoch())

        case Dead | PrepareEpochFence =>
          val errorMsg = s"Found transactionalId $transactionalId with state ${txnMetadata.state}. " +
            s"This is illegal as we should never have transitioned to this state."
          fatal(errorMsg)
          throw new IllegalStateException(errorMsg)
      }
    }
  }

  def handleListTransactions(
    filteredProducerIds: Set[Long],
    filteredStates: Set[String]
  ): ListTransactionsResponseData = {
    if (!isActive.get()) {
      new ListTransactionsResponseData().setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code)
    } else {
      txnManager.listTransactionStates(filteredProducerIds, filteredStates)
    }
  }

  def handleDescribeTransactions(
    transactionalId: String
  ): DescribeTransactionsResponseData.TransactionState = {
    if (transactionalId == null) {
      throw new IllegalArgumentException("Invalid null transactionalId")
    }

    val transactionState = new DescribeTransactionsResponseData.TransactionState()
      .setTransactionalId(transactionalId)

    if (!isActive.get()) {
      transactionState.setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code)
    } else if (transactionalId.isEmpty) {
      transactionState.setErrorCode(Errors.INVALID_REQUEST.code)
    } else {
      txnManager.getTransactionState(transactionalId) match {
        case Left(error) =>
          transactionState.setErrorCode(error.code)
        case Right(None) =>
          transactionState.setErrorCode(Errors.TRANSACTIONAL_ID_NOT_FOUND.code)
        case Right(Some(coordinatorEpochAndMetadata)) =>
          val txnMetadata = coordinatorEpochAndMetadata.transactionMetadata
          txnMetadata.inLock {
            if (txnMetadata.state == Dead) {
              // The transaction state is being expired, so ignore it
              transactionState.setErrorCode(Errors.TRANSACTIONAL_ID_NOT_FOUND.code)
            } else {
              txnMetadata.topicPartitions.foreach { topicPartition =>
                var topicData = transactionState.topics.find(topicPartition.topic)
                if (topicData == null) {
                  topicData = new DescribeTransactionsResponseData.TopicData()
                    .setTopic(topicPartition.topic)
                  transactionState.topics.add(topicData)
                }
                topicData.partitions.add(topicPartition.partition)
              }

              transactionState
                .setErrorCode(Errors.NONE.code)
                .setProducerId(txnMetadata.producerId)
                .setProducerEpoch(txnMetadata.producerEpoch)
                .setTransactionState(txnMetadata.state.name)
                .setTransactionTimeoutMs(txnMetadata.txnTimeoutMs)
                .setTransactionStartTimeMs(txnMetadata.txnStartTimestamp)
            }
          }
      }
    }
  }
  
  def handleVerifyPartitionsInTransaction(transactionalId: String,
                                          producerId: Long,
                                          producerEpoch: Short,
                                          partitions: collection.Set[TopicPartition],
                                          responseCallback: VerifyPartitionsCallback): Unit = {
    if (transactionalId == null || transactionalId.isEmpty) {
      debug(s"Returning ${Errors.INVALID_REQUEST} error code to client for $transactionalId's AddPartitions request for verification")
      responseCallback(AddPartitionsToTxnResponse.resultForTransaction(transactionalId, partitions.map(_ -> Errors.INVALID_REQUEST).toMap.asJava))
    } else {
      val result: ApiResult[Map[TopicPartition, Errors]] =
        txnManager.getTransactionState(transactionalId).flatMap {
          case None => Left(Errors.INVALID_PRODUCER_ID_MAPPING)

          case Some(epochAndMetadata) =>
            val txnMetadata = epochAndMetadata.transactionMetadata

            // Given the txnMetadata is valid, we check if the partitions are in the transaction.
            // Pending state is not checked since there is a final validation on the append to the log.
            // Partitions are added to metadata when the add partitions state is persisted, and removed when the end marker is persisted.
            txnMetadata.inLock {
              if (txnMetadata.producerId != producerId) {
                Left(Errors.INVALID_PRODUCER_ID_MAPPING)
              } else if (txnMetadata.producerEpoch != producerEpoch) {
                Left(Errors.PRODUCER_FENCED)
              } else if (txnMetadata.state == PrepareCommit || txnMetadata.state == PrepareAbort) {
                Left(Errors.CONCURRENT_TRANSACTIONS)
              } else {
                Right(partitions.map { part =>
                  if (txnMetadata.topicPartitions.contains(part))
                    (part, Errors.NONE)
                  else
                    (part, Errors.INVALID_TXN_STATE)
                }.toMap)
              }
            }
        }

      result match {
        case Left(err) =>
          debug(s"Returning $err error code to client for $transactionalId's AddPartitions request for verification")
          responseCallback(AddPartitionsToTxnResponse.resultForTransaction(transactionalId, partitions.map(_ -> err).toMap.asJava))
          
        case Right(errors) =>
          responseCallback(AddPartitionsToTxnResponse.resultForTransaction(transactionalId, errors.asJava))
      }
    }
    
  }

  def handleAddPartitionsToTransaction(transactionalId: String,
                                       producerId: Long,
                                       producerEpoch: Short,
                                       partitions: collection.Set[TopicPartition],
                                       responseCallback: AddPartitionsCallback,
                                       requestLocal: RequestLocal = RequestLocal.NoCaching): Unit = {
    if (transactionalId == null || transactionalId.isEmpty) {
      debug(s"Returning ${Errors.INVALID_REQUEST} error code to client for $transactionalId's AddPartitions request")
      responseCallback(Errors.INVALID_REQUEST)
    } else {
      // try to update the transaction metadata and append the updated metadata to txn log;
      // if there is no such metadata treat it as invalid producerId mapping error.
      val result: ApiResult[(Int, TxnTransitMetadata)] = txnManager.getTransactionState(transactionalId).flatMap {
        case None => Left(Errors.INVALID_PRODUCER_ID_MAPPING)

        case Some(epochAndMetadata) =>
          // 1. get old metadata first
          val coordinatorEpoch = epochAndMetadata.coordinatorEpoch
          val txnMetadata = epochAndMetadata.transactionMetadata

          // 2. generate the new transaction metadata with added partitions
          txnMetadata.inLock {
            // check producerId
            if (txnMetadata.producerId != producerId) {
              Left(Errors.INVALID_PRODUCER_ID_MAPPING)
            }
            // check producerEpoch
            else if (txnMetadata.producerEpoch != producerEpoch) {
              Left(Errors.PRODUCER_FENCED)
            }
            // check state
            else if (txnMetadata.pendingTransitionInProgress) {
              // return a retriable exception to let the client backoff and retry
              Left(Errors.CONCURRENT_TRANSACTIONS)
            } else if (txnMetadata.state == PrepareCommit || txnMetadata.state == PrepareAbort) {
              Left(Errors.CONCURRENT_TRANSACTIONS)
            } else if (txnMetadata.state == Ongoing && partitions.subsetOf(txnMetadata.topicPartitions)) {
              // this is an optimization: if the partitions are already in the metadata reply OK immediately
              Left(Errors.NONE)
            } else {
            // all check is okay
              Right(coordinatorEpoch, txnMetadata.prepareAddPartitions(partitions.toSet, time.milliseconds()))
            }
          }
      }

      result match {
        case Left(err) =>
          debug(s"Returning $err error code to client for $transactionalId's AddPartitions request")
          responseCallback(err)

        case Right((coordinatorEpoch, newMetadata)) =>
          txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, newMetadata,
            responseCallback, requestLocal = requestLocal)
      }
    }
  }

  /**
   * Load state from the given partition and begin handling requests for groups which map to this partition.
   *
   * @param txnTopicPartitionId The partition that we are now leading
   * @param coordinatorEpoch The partition coordinator (or leader) epoch from the received LeaderAndIsr request
   */
  def onElection(txnTopicPartitionId: Int, coordinatorEpoch: Int): Unit = {
    info(s"Elected as the txn coordinator for partition $txnTopicPartitionId at epoch $coordinatorEpoch")
    // The operations performed during immigration must be resilient to any previous errors we saw or partial state we
    // left off during the unloading phase. Ensure we remove all associated state for this partition before we continue
    // loading it.
    txnMarkerChannelManager.removeMarkersForTxnTopicPartition(txnTopicPartitionId)

    // Now load the partition.
    txnManager.loadTransactionsForTxnTopicPartition(txnTopicPartitionId, coordinatorEpoch,
      txnMarkerChannelManager.addTxnMarkersToSend)
  }

  /**
   * Clear coordinator caches for the given partition after giving up leadership.
   *
   * @param txnTopicPartitionId The partition that we are no longer leading
   * @param coordinatorEpoch The partition coordinator (or leader) epoch, which may be absent if we
   *                         are resigning after receiving a StopReplica request from the controller
   */
  def onResignation(txnTopicPartitionId: Int, coordinatorEpoch: Option[Int]): Unit = {
    info(s"Resigned as the txn coordinator for partition $txnTopicPartitionId at epoch $coordinatorEpoch")
    coordinatorEpoch match {
      case Some(epoch) =>
        txnManager.removeTransactionsForTxnTopicPartition(txnTopicPartitionId, epoch)
      case None =>
        txnManager.removeTransactionsForTxnTopicPartition(txnTopicPartitionId)
    }
    txnMarkerChannelManager.removeMarkersForTxnTopicPartition(txnTopicPartitionId)
  }

  private def logInvalidStateTransitionAndReturnError(transactionalId: String,
                                                      transactionState: TransactionState,
                                                      transactionResult: TransactionResult) = {
    debug(s"TransactionalId: $transactionalId's state is $transactionState, but received transaction " +
      s"marker result to send: $transactionResult")
    Left(Errors.INVALID_TXN_STATE)
  }

  def handleEndTransaction(transactionalId: String,
                           producerId: Long,
                           producerEpoch: Short,
                           txnMarkerResult: TransactionResult,
                           responseCallback: EndTxnCallback,
                           requestLocal: RequestLocal = RequestLocal.NoCaching): Unit = {
    endTransaction(transactionalId,
      producerId,
      producerEpoch,
      txnMarkerResult,
      isFromClient = true,
      responseCallback,
      requestLocal)
  }

  /**
   * handle EndTxnRequest
   * @param transactionalId
   * @param producerId
   * @param producerEpoch
   * @param txnMarkerResult
   * @param isFromClient
   * @param responseCallback
   * @param requestLocal
   */
  private def endTransaction(transactionalId: String,
                             producerId: Long,
                             producerEpoch: Short,
                             txnMarkerResult: TransactionResult,
                             isFromClient: Boolean,
                             responseCallback: EndTxnCallback,
                             requestLocal: RequestLocal): Unit = {
    var isEpochFence = false
    // step 1
    // check transactionalId
    if (transactionalId == null || transactionalId.isEmpty)
      responseCallback(Errors.INVALID_REQUEST)
    else {
      // get info from __transaction_state
      val preAppendResult: ApiResult[(Int, TxnTransitMetadata)] = txnManager.getTransactionState(transactionalId).flatMap {
        case None =>
          Left(Errors.INVALID_PRODUCER_ID_MAPPING)

        case Some(epochAndTxnMetadata) =>
          // get from CoordinatorEpochAndTxnMetadata
          val txnMetadata = epochAndTxnMetadata.transactionMetadata
          val coordinatorEpoch = epochAndTxnMetadata.coordinatorEpoch

          txnMetadata.inLock {
            // case1: Errors
            if (txnMetadata.producerId != producerId)
              Left(Errors.INVALID_PRODUCER_ID_MAPPING)
            // Strict equality is enforced on the client side requests, as they shouldn't bump the producer epoch.
            // case2: Errors
            else if ((isFromClient && producerEpoch != txnMetadata.producerEpoch) || producerEpoch < txnMetadata.producerEpoch)
              Left(Errors.PRODUCER_FENCED)
            // case3: Errors
            else if (txnMetadata.pendingTransitionInProgress && txnMetadata.pendingState.get != PrepareEpochFence)
              Left(Errors.CONCURRENT_TRANSACTIONS)
            // case4: Normal
            else txnMetadata.state match {
              // case4.1
              case Ongoing =>
                // check "Commit" or "Abort" to commit?
                val nextState = if (txnMarkerResult == TransactionResult.COMMIT)
                  PrepareCommit
                else
                  PrepareAbort
                // PrepareAbort handle
                if (nextState == PrepareAbort && txnMetadata.pendingState.contains(PrepareEpochFence)) {
                  // We should clear the pending state to make way for the transition to PrepareAbort and also bump
                  // the epoch in the transaction metadata we are about to append.
                  isEpochFence = true
                  txnMetadata.pendingState = None
                  txnMetadata.producerEpoch = producerEpoch
                  txnMetadata.lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH
                }

                Right(coordinatorEpoch, txnMetadata.prepareAbortOrCommit(nextState, time.milliseconds()))
              // case4.2
              case CompleteCommit =>
                if (txnMarkerResult == TransactionResult.COMMIT)
                  Left(Errors.NONE)
                else
                  logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
              // case4.3
              case CompleteAbort =>
                if (txnMarkerResult == TransactionResult.ABORT)
                  Left(Errors.NONE)
                else
                  logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
              // case4.4
              case PrepareCommit =>
                if (txnMarkerResult == TransactionResult.COMMIT)
                  Left(Errors.CONCURRENT_TRANSACTIONS)
                else
                  logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
              // case4.5
              case PrepareAbort =>
                if (txnMarkerResult == TransactionResult.ABORT)
                  Left(Errors.CONCURRENT_TRANSACTIONS)
                else
                  logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
              // case4.6
              case Empty =>
                logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
              // case4.7
              case Dead | PrepareEpochFence =>
                val errorMsg = s"Found transactionalId $transactionalId with state ${txnMetadata.state}. " +
                  s"This is illegal as we should never have transitioned to this state."
                fatal(errorMsg)
                throw new IllegalStateException(errorMsg)

            }
          }
      }

      // step 2
      preAppendResult match {
        case Left(err) =>
          debug(s"Aborting append of $txnMarkerResult to transaction log with coordinator and returning $err error to client for $transactionalId's EndTransaction request")
          responseCallback(err)
        // Going here means "TransactionCoordinator can switch to PrepareCommit state"
        case Right((coordinatorEpoch, newMetadata)) =>
          // define "sendTxnMarkers" Callback
          def sendTxnMarkersCallback(error: Errors): Unit = {
            if (error == Errors.NONE) {
              // check "TransactionCoordinator can switch to CompleteCommit state" ?
              val preSendResult: ApiResult[(TransactionMetadata, TxnTransitMetadata)] = txnManager.getTransactionState(transactionalId).flatMap {
                case None =>
                  val errorMsg = s"The coordinator still owns the transaction partition for $transactionalId, but there is " +
                    s"no metadata in the cache; this is not expected"
                  fatal(errorMsg)
                  throw new IllegalStateException(errorMsg)

                case Some(epochAndMetadata) =>
                  if (epochAndMetadata.coordinatorEpoch == coordinatorEpoch) {
                    val txnMetadata = epochAndMetadata.transactionMetadata
                    txnMetadata.inLock {
                      if (txnMetadata.producerId != producerId)
                        Left(Errors.INVALID_PRODUCER_ID_MAPPING)
                      else if (txnMetadata.producerEpoch != producerEpoch)
                        Left(Errors.PRODUCER_FENCED)
                      else if (txnMetadata.pendingTransitionInProgress)
                        Left(Errors.CONCURRENT_TRANSACTIONS)
                      else txnMetadata.state match {
                        case Empty| Ongoing | CompleteCommit | CompleteAbort =>
                          logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
                        case PrepareCommit =>
                          if (txnMarkerResult != TransactionResult.COMMIT)
                            logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
                          else
                            Right(txnMetadata, txnMetadata.prepareComplete(time.milliseconds()))
                        case PrepareAbort =>
                          if (txnMarkerResult != TransactionResult.ABORT)
                            logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
                          else
                            Right(txnMetadata, txnMetadata.prepareComplete(time.milliseconds()))
                        case Dead | PrepareEpochFence =>
                          val errorMsg = s"Found transactionalId $transactionalId with state ${txnMetadata.state}. " +
                            s"This is illegal as we should never have transitioned to this state."
                          fatal(errorMsg)
                          throw new IllegalStateException(errorMsg)

                      }
                    }
                  } else {
                    debug(s"The transaction coordinator epoch has changed to ${epochAndMetadata.coordinatorEpoch} after $txnMarkerResult was " +
                      s"successfully appended to the log for $transactionalId with old epoch $coordinatorEpoch")
                    Left(Errors.NOT_COORDINATOR)
                  }
              }

              preSendResult match {
                case Left(err) =>
                  info(s"Aborting sending of transaction markers after appended $txnMarkerResult to transaction log and returning $err error to client for $transactionalId's EndTransaction request")
                  responseCallback(err)
                // Going here means "TransactionCoordinator can switch to CompleteCommit state"?
                case Right((txnMetadata, newPreSendMetadata)) =>
                  // we can respond to the client immediately and continue to write the txn markers if
                  // the log append was successful
                  responseCallback(Errors.NONE)
                  // "WriteTxnMarkers Request" attach
                  txnMarkerChannelManager.addTxnMarkersToSend(coordinatorEpoch, txnMarkerResult, txnMetadata, newPreSendMetadata)
              }
            } else {
              info(s"Aborting sending of transaction markers and returning $error error to client for $transactionalId's EndTransaction request of $txnMarkerResult, " +
                s"since appending $newMetadata to transaction log with coordinator epoch $coordinatorEpoch failed")

              if (isEpochFence) {
                txnManager.getTransactionState(transactionalId).foreach {
                  case None =>
                    warn(s"The coordinator still owns the transaction partition for $transactionalId, but there is " +
                      s"no metadata in the cache; this is not expected")

                  case Some(epochAndMetadata) =>
                    if (epochAndMetadata.coordinatorEpoch == coordinatorEpoch) {
                      // This was attempted epoch fence that failed, so mark this state on the metadata
                      epochAndMetadata.transactionMetadata.hasFailedEpochFence = true
                      warn(s"The coordinator failed to write an epoch fence transition for producer $transactionalId to the transaction log " +
                        s"with error $error. The epoch was increased to ${newMetadata.producerEpoch} but not returned to the client")
                    }
                }
              }

              responseCallback(error)
            }
          }

          // step 3: try to append "PrepareCommit" state to __transaction_state
          // if append success, will call  sendTxnMarkersCallback, define in step 2
          txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, newMetadata,
            sendTxnMarkersCallback, requestLocal = requestLocal)
      }
    }
  }

  def transactionTopicConfigs: Properties = txnManager.transactionTopicConfigs

  def partitionFor(transactionalId: String): Int = txnManager.partitionFor(transactionalId)

  private def onEndTransactionComplete(txnIdAndPidEpoch: TransactionalIdAndProducerIdEpoch)(error: Errors): Unit = {
    error match {
      case Errors.NONE =>
        info("Completed rollback of ongoing transaction for transactionalId " +
          s"${txnIdAndPidEpoch.transactionalId} due to timeout")

      case error@(Errors.INVALID_PRODUCER_ID_MAPPING |
                  Errors.PRODUCER_FENCED |
                  Errors.CONCURRENT_TRANSACTIONS) =>
        debug(s"Rollback of ongoing transaction for transactionalId ${txnIdAndPidEpoch.transactionalId} " +
          s"has been cancelled due to error $error")

      case error =>
        warn(s"Rollback of ongoing transaction for transactionalId ${txnIdAndPidEpoch.transactionalId} " +
          s"failed due to error $error")
    }
  }

  private[transaction] def abortTimedOutTransactions(onComplete: TransactionalIdAndProducerIdEpoch => EndTxnCallback): Unit = {

    txnManager.timedOutTransactions().foreach { txnIdAndPidEpoch =>
      txnManager.getTransactionState(txnIdAndPidEpoch.transactionalId).foreach {
        case None =>
          error(s"Could not find transaction metadata when trying to timeout transaction for $txnIdAndPidEpoch")

        case Some(epochAndTxnMetadata) =>
          val txnMetadata = epochAndTxnMetadata.transactionMetadata
          val transitMetadataOpt = txnMetadata.inLock {
            if (txnMetadata.producerId != txnIdAndPidEpoch.producerId) {
              error(s"Found incorrect producerId when expiring transactionalId: ${txnIdAndPidEpoch.transactionalId}. " +
                s"Expected producerId: ${txnIdAndPidEpoch.producerId}. Found producerId: " +
                s"${txnMetadata.producerId}")
              None
            } else if (txnMetadata.pendingTransitionInProgress) {
              debug(s"Skipping abort of timed out transaction $txnIdAndPidEpoch since there is a " +
                "pending state transition")
              None
            } else {
              Some(txnMetadata.prepareFenceProducerEpoch())
            }
          }

          transitMetadataOpt.foreach { txnTransitMetadata =>
            endTransaction(txnMetadata.transactionalId,
              txnTransitMetadata.producerId,
              txnTransitMetadata.producerEpoch,
              TransactionResult.ABORT,
              isFromClient = false,
              onComplete(txnIdAndPidEpoch),
              RequestLocal.NoCaching)
          }
      }
    }
  }

  /**
   * Startup logic executed at the same time when the server starts up.
   */
  def startup(retrieveTransactionTopicPartitionCount: () => Int, enableTransactionalIdExpiration: Boolean = true): Unit = {
    info("Starting up.")
    scheduler.startup()
    scheduler.schedule("transaction-abort",
      () => abortTimedOutTransactions(onEndTransactionComplete),
      txnConfig.abortTimedOutTransactionsIntervalMs,
      txnConfig.abortTimedOutTransactionsIntervalMs
    )
    txnManager.startup(retrieveTransactionTopicPartitionCount, enableTransactionalIdExpiration)
    txnMarkerChannelManager.start()
    isActive.set(true)

    info("Startup complete.")
  }

  /**
   * Shutdown logic executed at the same time when server shuts down.
   * Ordering of actions should be reversed from the startup process.
   */
  def shutdown(): Unit = {
    info("Shutting down.")
    isActive.set(false)
    scheduler.shutdown()
    producerIdManager.shutdown()
    txnManager.shutdown()
    txnMarkerChannelManager.shutdown()
    info("Shutdown complete.")
  }
}

case class InitProducerIdResult(producerId: Long, producerEpoch: Short, error: Errors)
