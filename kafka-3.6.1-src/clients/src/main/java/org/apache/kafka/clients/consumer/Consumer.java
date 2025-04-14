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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * @see KafkaConsumer
 * @see MockConsumer
 */
public interface Consumer<K, V> extends Closeable {

    /**
     * 获取分配给该Consumer的TopicPartition
     * @see KafkaConsumer#assignment()
     */
    Set<TopicPartition> assignment();

    /**
     * subscribe模式下：subscribe(Collection, ConsumerRebalanceListener)
     * 即自己订阅的 Collection
     * @see KafkaConsumer#subscription()
     */
    Set<String> subscription();

    // ------------------------------------------------ subscribe "订阅" 模式 ---------------------------------------
    /**
     * Subscribe to the given list of topics to get dynamically assigned partitions.
     * @see KafkaConsumer#subscribe(Collection)
     */
    void subscribe(Collection<String> topics);

    /**
     * subscribe模式下“订阅”方法：
     * Subscribe to the given list of topics to get dynamically assigned partitions.
     * The provided listener will be invoked first to indicate that the consumer's assignment has been revoked, and then again when the new assignment has been received.
     * @see KafkaConsumer#subscribe(Collection, ConsumerRebalanceListener)
     */
    void subscribe(Collection<String> topics, ConsumerRebalanceListener callback);

    /**
    * @see KafkaConsumer#subscribe(Pattern, ConsumerRebalanceListener)
    */
    void subscribe(Pattern pattern, ConsumerRebalanceListener callback);

    /**
    * @see KafkaConsumer#subscribe(Pattern)
    */
    void subscribe(Pattern pattern);

    /**
     * @see KafkaConsumer#unsubscribe()
     */
    void unsubscribe();

    // ------------------------------------------------ assign "订阅" 模式 ---------------------------------------
    /**
     * assign模式下“订阅”方法
     * @see KafkaConsumer#assign(Collection)
     */
    void assign(Collection<TopicPartition> partitions);


    // ------------------------------------------------ 消费的方法 ---------------------------------------
    /**
     * @see KafkaConsumer#poll(long)
     */
    @Deprecated
    ConsumerRecords<K, V> poll(long timeout);

    /**
     * @see KafkaConsumer#poll(Duration)
     */
    ConsumerRecords<K, V> poll(Duration timeout);

    // ------------------------------------------------ consumed offset 提交的方法 ------------------------
    /**
     * @see KafkaConsumer#commitSync()
     */
    void commitSync();

    /**
     * @see KafkaConsumer#commitSync(Duration)
     */
    void commitSync(Duration timeout);

    /**
     * @see KafkaConsumer#commitSync(Map)
     */
    void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets);

    /**
     * @see KafkaConsumer#commitSync(Map, Duration)
     */
    void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets, final Duration timeout);
    /**
     * @see KafkaConsumer#commitAsync()
     */
    void commitAsync();

    /**
     * @see KafkaConsumer#commitAsync(OffsetCommitCallback)
     */
    void commitAsync(OffsetCommitCallback callback);

    /**
     * @see KafkaConsumer#commitAsync(Map, OffsetCommitCallback)
     */
    void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback);

    // ------------------------------------------------ seek 至某个offset的方法 ------------------------
    /**
     * @see KafkaConsumer#seek(TopicPartition, long)
     */
    void seek(TopicPartition partition, long offset);

    /**
     * @see KafkaConsumer#seek(TopicPartition, OffsetAndMetadata)
     */
    void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata);

    /**
     * @see KafkaConsumer#seekToBeginning(Collection)
     */
    void seekToBeginning(Collection<TopicPartition> partitions);

    /**
     * @see KafkaConsumer#seekToEnd(Collection)
     */
    void seekToEnd(Collection<TopicPartition> partitions);

    // --------------------------------------- 获取 要消费的TopicPartition 的下一个待消费/已提交的 offset ---------
    /**
     * Get the offset of the <i>next record</i> that will be fetched
     * @see KafkaConsumer#position(TopicPartition)
     */
    long position(TopicPartition partition);
    
    /**
     * @see KafkaConsumer#position(TopicPartition, Duration)
     */
    long position(TopicPartition partition, final Duration timeout);

    /**
     * @see KafkaConsumer#committed(TopicPartition)
     */
    @Deprecated
    OffsetAndMetadata committed(TopicPartition partition);

    /**
     * @see KafkaConsumer#committed(TopicPartition, Duration)
     */
    @Deprecated
    OffsetAndMetadata committed(TopicPartition partition, final Duration timeout);

    /**
     * Get the last committed offsets for the given partitions
     * @see KafkaConsumer#committed(Set)
     */
    Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions);

    /**
     * @see KafkaConsumer#committed(Set, Duration)
     */
    Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions, final Duration timeout);

    // --------------------------------------- 获取 consumer 相关 Metric ---------
    /**
     * @see KafkaConsumer#metrics()
     */
    Map<MetricName, ? extends Metric> metrics();


    // --------------------------------------- 获取 订阅的单个/全部 Topic的元数据 ---------
    /**
     * 获取单个topic的元数据
     * @see KafkaConsumer#partitionsFor(String)
     */
    List<PartitionInfo> partitionsFor(String topic);

    /**
     * @see KafkaConsumer#partitionsFor(String, Duration)
     */
    List<PartitionInfo> partitionsFor(String topic, Duration timeout);

    /**
     * 获取全部topic的元数据
     * @see KafkaConsumer#listTopics()
     */
    Map<String, List<PartitionInfo>> listTopics();

    /**
     * @see KafkaConsumer#listTopics(Duration)
     */
    Map<String, List<PartitionInfo>> listTopics(Duration timeout);

    // --------------------------------------- 暂停/重启 TopicPartitions 的消费 ---------
    /**
     * @see KafkaConsumer#paused()
     */
    Set<TopicPartition> paused();

    /**
     * @see KafkaConsumer#pause(Collection)
     */
    void pause(Collection<TopicPartition> partitions);

    /**
     * @see KafkaConsumer#resume(Collection)
     */
    void resume(Collection<TopicPartition> partitions);

    // --------------------------------------- 按不同维度查找 Offset ---------
    /**
     * @see KafkaConsumer#offsetsForTimes(Map)
     */
    Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch);

    /**
     * @see KafkaConsumer#offsetsForTimes(Map, Duration)
     */
    Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout);

    /**
     * @see KafkaConsumer#beginningOffsets(Collection)
     */
    Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions);

    /**
     * @see KafkaConsumer#beginningOffsets(Collection, Duration)
     */
    Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout);

    /**
     * @see KafkaConsumer#endOffsets(Collection)
     */
    Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions);

    /**
     * @see KafkaConsumer#endOffsets(Collection, Duration)
     */
    Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout);

    // --------------------------------------- 获取针对某TopicPartition的Lag ---------------------
    /**
     * @see KafkaConsumer#currentLag(TopicPartition)
     */
    OptionalLong currentLag(TopicPartition topicPartition);

    // --------------------------------------- 获取 consumer 的元数据 ---------
    /**
     * @see KafkaConsumer#groupMetadata()
     */
    ConsumerGroupMetadata groupMetadata();

    // --------------------------------------- 强制触发 Rebalance ---------
    /**
     * @see KafkaConsumer#enforceRebalance()
     */
    void enforceRebalance();

    /**
     * @see KafkaConsumer#enforceRebalance(String)
     */
    void enforceRebalance(final String reason);

    // --------------------------------------- 关闭KafkaConsumer ---------
    /**
     * @see KafkaConsumer#close()
     */
    void close();

    /**
     * @see KafkaConsumer#close(Duration)
     */
    void close(Duration timeout);

    // ---------------------- This method is thread-safe and is useful in particular to abort a long poll. ---------
    /**
     * @see KafkaConsumer#wakeup()
     */
    void wakeup();

}
