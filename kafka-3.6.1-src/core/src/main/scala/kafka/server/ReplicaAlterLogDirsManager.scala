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

import kafka.cluster.BrokerEndPoint
import org.apache.kafka.common.TopicPartition

class ReplicaAlterLogDirsManager(brokerConfig: KafkaConfig,
                                 replicaManager: ReplicaManager,
                                 quotaManager: ReplicationQuotaManager,
                                 brokerTopicStats: BrokerTopicStats)
  extends AbstractFetcherManager[ReplicaAlterLogDirsThread](
    name = s"ReplicaAlterLogDirsManager on broker ${brokerConfig.brokerId}",
    clientId = "ReplicaAlterLogDirs",
    numFetchers = brokerConfig.getNumReplicaAlterLogDirsThreads) {
  // Option(getInt(KafkaConfig.NumReplicaAlterLogDirsThreadsProp)).getOrElse(logDirs.size)
  // num.replica.alter.log.dirs.threads 没给默认值, 用户不填，就是log.dirs配置的磁盘的数量
  // move replicas between log directories 用于在log.dirs存储目录间移动Replica

  override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): ReplicaAlterLogDirsThread = {
    val threadName = s"ReplicaAlterLogDirsThread-$fetcherId"
    val leader = new LocalLeaderEndPoint(sourceBroker, brokerConfig, replicaManager, quotaManager)
    new ReplicaAlterLogDirsThread(threadName, leader, failedPartitions, replicaManager,
      quotaManager, brokerTopicStats, brokerConfig.replicaFetchBackoffMs)
  }

  override protected def addPartitionsToFetcherThread(fetcherThread: ReplicaAlterLogDirsThread,
                                                      initialOffsetAndEpochs: collection.Map[TopicPartition, InitialFetchState]): Unit = {
    val addedPartitions = fetcherThread.addPartitions(initialOffsetAndEpochs)
    val (addedInitialOffsets, notAddedInitialOffsets) = initialOffsetAndEpochs.partition { case (tp, _) =>
      addedPartitions.contains(tp)
    }

    if (addedInitialOffsets.nonEmpty)
      info(s"Added log dir fetcher for partitions with initial offsets $addedInitialOffsets")

    if (notAddedInitialOffsets.nonEmpty)
      info(s"Failed to add log dir fetch for partitions ${notAddedInitialOffsets.keySet} " +
        s"since the log dir reassignment has already completed")
  }

  def shutdown(): Unit = {
    info("shutting down")
    closeAllFetchers()
    info("shutdown completed")
  }
}
