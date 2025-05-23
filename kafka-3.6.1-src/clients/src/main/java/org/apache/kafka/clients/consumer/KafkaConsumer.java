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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.internals.ConsumerCoordinator;
import org.apache.kafka.clients.consumer.internals.ConsumerInterceptors;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.consumer.internals.Fetch;
import org.apache.kafka.clients.consumer.internals.FetchConfig;
import org.apache.kafka.clients.consumer.internals.FetchMetricsManager;
import org.apache.kafka.clients.consumer.internals.Fetcher;
import org.apache.kafka.clients.consumer.internals.KafkaConsumerMetrics;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.OffsetFetcher;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.clients.consumer.internals.TopicMetadataFetcher;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_JMX_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createConsumerInterceptors;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createConsumerNetworkClient;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createFetchConfig;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createFetchMetricsManager;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createIsolationLevel;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createKeyDeserializer;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createLogContext;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createSubscriptionState;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createValueDeserializer;

/**
 * A client that consumes records from a Kafka cluster.
 * <p>
 * 1. 如果 broker 故障 或者 TopicPartition迁移，客户端能够“透明”的感知处理掉~
 * This client transparently handles the failure of Kafka brokers, and transparently adapts as topic partitions
 * it fetches migrate within the cluster. This client also interacts with the broker to allow groups of
 * consumers to load balance consumption using <a href="#consumergroups">consumer groups</a>.
 * <p>
 *
 * 2. TCP连接的管理
 * The consumer maintains TCP connections to the necessary brokers to fetch data.
 * Failure to close the consumer after use will leak these connections.
 *
 * 3. 不是线程安全的
 * The consumer is not thread-safe. See <a href="#multithreaded">Multi-threaded Processing</a> for more details.
 *
 * 4. 与broker版本的兼容说明
 * <h3>Cross-Version Compatibility</h3>
 * This client can communicate with brokers that are version 0.10.0 or newer. Older or newer brokers may not support
 * certain features. For example, 0.10.0 brokers do not support offsetsForTimes, because this feature was added
 * in version 0.10.1. You will receive an {@link org.apache.kafka.common.errors.UnsupportedVersionException}
 * when invoking an API that is not available on the running broker version.
 * <p>
 *
 *  5. “Consumed Offset” 说明
 * <h3>Offsets and Consumer Position</h3>
 * Kafka maintains a numerical offset for each record in a partition. This offset acts as a unique identifier of
 * a record within that partition, and also denotes the position of the consumer in the partition. For example, a consumer
 * which is at position 5 has consumed records with offsets 0 through 4 and will next receive the record with offset 5. There
 * are actually two notions of position relevant to the user of the consumer:
 * <p>
 * The {@link #position(TopicPartition) position} of the consumer gives the offset of the next record that will be given
 * out. It will be one larger than the highest offset the consumer has seen in that partition. It automatically advances
 * every time the consumer receives messages in a call to {@link #poll(Duration)}.
 * <p>
 * - The {@link #commitSync() committed position} is the last offset that has been stored securely.
 * - Should the process fail and restart, this is the offset that the consumer will recover to.
 * - The consumer can either automatically commit offsets periodically;
 * - or it can choose to control this committed position manually by calling one of the commit APIs (e.g. {@link #commitSync() commitSync} and {@link #commitAsync(OffsetCommitCallback) commitAsync}).
 * <p>
 * This distinction gives the consumer control over when a record is considered consumed. It is discussed in further
 * detail below.
 *
 * 6. “Consumer Groups” 说明
 * <h3><a name="consumergroups">Consumer Groups and Topic Subscriptions</a></h3>
 *
 * 6.1 相同的group.id标志着同一 Consumer Group 下的 Consumer Member
 * Kafka uses the concept of <i>consumer groups</i> to allow a pool of processes to divide the work of consuming and
 * processing records. These processes can either be running on the same machine or they can be
 * distributed over many machines to provide scalability and fault tolerance for processing. All consumer instances
 * sharing the same {@code group.id} will be part of the same consumer group.
 *
 * <p>
 * 6.2
 * - 每个consumer member 都可以使用 subscribe 这一 API 来动态的设置它想消费的 topics
 * - 1条“消息”，只会被消费者组中的 一个消费者收到
 * - 一个Partition 只能分配给 组内 的一个消费者
 * ↓
 * Each consumer in a group can dynamically set the list of topics it wants to subscribe to through one of the
 * {@link #subscribe(Collection, ConsumerRebalanceListener) subscribe} APIs. Kafka will deliver each message in the
 * subscribed topics to one process in each consumer group. This is achieved by balancing the partitions between all
 * members in the consumer group so that each partition is assigned to exactly one consumer in the group. So if there
 * is a topic with four partitions, and a consumer group with two processes, each process would consume from two partitions.
 * <p>
 *
 * - “Membership” 这里可以理解为 Consumer Group 中 TopicPartition 与 Consumer Member的绑定关系
 * - case1：一个 原有的“Consumer Member” “失败了” -> 它原本持有的TopicPartition 会与 其它 Consumer Member 建立绑定关系
 * - case2：一个 新来的“Consumer Member” “成功加入了” -> 原本在组内的一个Consumer Member 会“放弃”其所持有的TopicPartition，交给新来的
 * - case3：“subscribed topics”中有Topic，增加了TopicPartition
 * - case4：以subscribe(Pattern) 模式订阅时，有新的Topic匹配上
 * - case3和case4 本质是 有新的TopicPartition出现了，是如何识别的呢？ -> consumer group会定期的刷新 metadata，然后assign给 consumer member
 * ↓
 * Membership in a consumer group is maintained dynamically: if a process fails, the partitions assigned to it will
 * be reassigned to other consumers in the same group. Similarly, if a new consumer joins the group, partitions will be moved
 * from existing consumers to the new one. This is known as <i>rebalancing</i> the group and is discussed in more
 * detail <a href="#failuredetection">below</a>. Group rebalancing is also used when new partitions are added
 * to one of the subscribed topics or when a new topic matching a {@link #subscribe(Pattern, ConsumerRebalanceListener) subscribed regex}
 * is created. The group will automatically detect the new partitions through periodic metadata refreshes and
 * assign them to members of the group.
 * <p>
 *
 * - 从“概念上”来看，consumer group 可以被视作为 由多个进程组成的 “一个逻辑上的订阅者”
 * - 一个Topic支持被多个 consumer group消费
 * ↓
 * Conceptually you can think of a consumer group as being a single logical subscriber that happens to be made up of
 * multiple processes. As a multi-subscriber system, Kafka naturally supports having any number of consumer groups for a
 * given topic without duplicating data (additional consumers are actually quite cheap).
 * <p>
 * This is a slight generalization of the functionality that is common in messaging systems. To get semantics similar to
 * a queue in a traditional messaging system all processes would be part of a single consumer group and hence record
 * delivery would be balanced over the group like with a queue. Unlike a traditional messaging system, though, you can
 * have multiple such groups. To get semantics similar to pub-sub in a traditional messaging system each process would
 * have its own consumer group, so each process would subscribe to all the records published to the topic.
 * <p>
 *
 * - consumer member 可以通过 ConsumerRebalanceListener 来感知到 “TopicPartition 重新分配“ 场景的发生
 * - 感知到 reassignment 行为发生之后，可做一些事情，比如”状态清理“，”手动提交offset“
 * ↓
 * In addition, when group reassignment happens automatically, consumers can be notified through a {@link ConsumerRebalanceListener},
 * which allows them to finish necessary application-level logic such as state cleanup, manual offset
 * commits, etc. See <a href="#rebalancecallback">Storing Offsets Outside Kafka</a> for more details.
 * <p>
 *
 *  - 除了subscribe之外，还支持assign的分配模式
 *  - assign模式不支持 动态 Partition 分配，也没了 ”组“ 的能力
 *  ↓
 * It is also possible for the consumer to <a href="#manualassignment">manually assign</a> specific partitions
 * (similar to the older "simple" consumer) using {@link #assign(Collection)}. In this case, dynamic partition
 * assignment and consumer group coordination will be disabled.
 *
 * <h3><a name="failuredetection">Detecting Consumer Failures</a></h3>
 * [- 如何发现一个 Consumer Member 的 ‘fail’ -]
 *  - subscribe api -> 加入了 consumer group
 *  - poll -> 消费数据 + 存活性的证明[heartbeat]
 *  - consumer member fail  -> 其下拥有的 TopicPartition 会被 reassign 给其他的 consumer
 *  ↓
 * After subscribing to a set of topics, the consumer will automatically join the group when {@link #poll(Duration)} is
 * invoked. The poll API is designed to ensure consumer liveness. As long as you continue to call poll, the consumer
 * will stay in the group and continue to receive messages from the partitions it was assigned. Underneath the covers,
 * the consumer sends periodic heartbeats to the server. If the consumer crashes or is unable to send heartbeats for
 * a duration of {@code session.timeout.ms}, then the consumer will be considered dead and its partitions will
 * be reassigned.
 * <p>
 *
 * - consumer member的活锁现象，以及为什么要有 ”max.poll.interval.ms“ 这一个配置 ？
 *   -> 避免 consumer 的 活锁情况(它持续发送heartbeatRequest，持有着TopicPartition，但是不消费，即不发送 FetchRequest)
 *   -> max.poll.interval.ms 是对上面情况的一种检测机制，检测到这种情况，consumer会主动离开组[发送leavegrouprequest]
 *   -> 这种情况下，会出现CommitFailedException，目的是确保只有组中的活跃成员才能提交偏移量，这种“活锁”现象标志着consumer member并不活跃
 *   -> 要想保留在consumer group中，就必须一直调用poll，发送heartbeat request
 *   ↓
 * It is also possible that the consumer could encounter a "livelock" situation where it is continuing
 * to send heartbeats, but no progress is being made. To prevent the consumer from holding onto its partitions
 * indefinitely in this case, we provide a liveness detection mechanism using the {@code max.poll.interval.ms}
 * setting. Basically if you don't call poll at least as frequently as the configured max interval,
 * then the client will proactively leave the group so that another consumer can take over its partitions. When this happens,
 * you may see an offset commit failure (as indicated by a {@link CommitFailedException} thrown from a call to {@link #commitSync()}).
 * This is a safety mechanism which guarantees that only active members of the group are able to commit offsets.
 * So to stay in the group, you must continue to call poll.
 * <p>
 * The consumer provides two configuration settings to control the behavior of the poll loop:
 *
 * “max.poll.interval.ms”和“max.poll.records”，本质是为了保证“存活性”和“吞吐量”
 * <ol>
 *     <li><code>max.poll.interval.ms</code>: By increasing the interval between expected polls, you can give
 *     the consumer more time to handle a batch of records returned from {@link #poll(Duration)}. The drawback
 *     is that increasing this value may delay a group rebalance since the consumer will only join the rebalance
 *     inside the call to poll. You can use this setting to bound the time to finish a rebalance, but
 *     you risk slower progress if the consumer cannot actually call {@link #poll(Duration) poll} often enough.</li>
 *     <li><code>max.poll.records</code>: Use this setting to limit the total records returned from a single
 *     call to poll. This can make it easier to predict the maximum that must be handled within each poll
 *     interval. By tuning this value, you may be able to reduce the poll interval, which will reduce the
 *     impact of group rebalancing.</li>
 * </ol>
 * <p>
 *
 * - 尽管有上面2个参数来调节，但是由于 消息的处理时间 是“不可预测”的，所以只靠这2个参数，其实是不够用的
 * - 推荐做法是，”message“ 处理的逻辑，应该交给另一个线程来做，这种做法需要禁用自动提交，在确认处理完消息之后手动提交
 * ↓
 * For use cases where message processing time varies unpredictably, neither of these options may be sufficient.
 * The recommended way to handle these cases is to move message processing to another thread, which allows
 * the consumer to continue calling {@link #poll(Duration) poll} while the processor is still working.
 * Some care must be taken to ensure that committed offsets do not get ahead of the actual position.
 * Typically, you must disable automatic commits and manually commit processed offsets for records only after the
 * thread has finished handling them (depending on the delivery semantics you need).
 * Note also that you will need to {@link #pause(Collection) pause} the partition so that no new records are received
 * from poll until after thread has finished handling those previously returned.
 *
 * <h3>Usage Examples</h3>
 * The consumer APIs offer flexibility to cover a variety of consumption use cases. Here are some examples to
 * demonstrate how to use them.
 *
 * ----------- 自动处理offset的示例 ----------------
 * <h4>Automatic Offset Committing</h4>
 * This example demonstrates a simple usage of Kafka's consumer api that relies on automatic offset committing.
 * <p>
 * <pre>
 *     Properties props = new Properties();
 *     props.setProperty(&quot;bootstrap.servers&quot;, &quot;localhost:9092&quot;);
 *     props.setProperty(&quot;group.id&quot;, &quot;test&quot;);
 *
 *     - 2个相关的配置参数 [ enable.auto.commit ] + [ auto.commit.interval.ms ]
 *     props.setProperty(&quot;enable.auto.commit&quot;, &quot;true&quot;);
 *     props.setProperty(&quot;auto.commit.interval.ms&quot;, &quot;1000&quot;);
 *     props.setProperty(&quot;key.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     props.setProperty(&quot;value.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     KafkaConsumer&lt;String, String&gt; consumer = new KafkaConsumer&lt;&gt;(props);
 *     consumer.subscribe(Arrays.asList(&quot;foo&quot;, &quot;bar&quot;));
 *     while (true) {
 *         - offsetCommitRequest 自动完成
 *         ConsumerRecords&lt;String, String&gt; records = consumer.poll(Duration.ofMillis(100));
 *         for (ConsumerRecord&lt;String, String&gt; record : records)
 *             System.out.printf(&quot;offset = %d, key = %s, value = %s%n&quot;, record.offset(), record.key(), record.value());
 *     }
 * </pre>
 *
 *  - bootstrap.servers的说明
 *  ↓
 * The connection to the cluster is bootstrapped by specifying a list of one or more brokers to contact using the
 * configuration {@code bootstrap.servers}. This list is just used to discover the rest of the brokers in the
 * cluster and need not be an exhaustive list of servers in the cluster (though you may want to specify more than one in
 * case there are servers down when the client is connecting).
 * <p>
 *
 *  - [ enable.auto.commit ] + [ auto.commit.interval.ms ] 的说明
 *  ↓
 * Setting {@code enable.auto.commit} means that offsets are committed automatically with a frequency controlled by
 * the config {@code auto.commit.interval.ms}.
 * <p>
 * In this example the consumer is subscribing to the topics <i>foo</i> and <i>bar</i> as part of a group of consumers
 * called <i>test</i> as configured with {@code group.id}.
 * <p>
 * The deserializer settings specify how to turn bytes into objects. For example, by specifying string deserializers, we
 * are saying that our record's key and value will just be simple strings.
 *
 *
 * ----------- 手动处理offset的示例 ----------------
 * <h4>Manual Offset Control</h4>
 *
 *  在message处理完成之前，不应将其视为已 ”consumed“
 * Instead of relying on the consumer to periodically commit consumed offsets, users can also control when records
 * should be considered as consumed and hence commit their offsets. This is useful when the consumption of the messages
 * is coupled with some processing logic and hence a message should not be considered as consumed until it is completed processing.

 * <p>
 * <pre>
 *     Properties props = new Properties();
 *     props.setProperty(&quot;bootstrap.servers&quot;, &quot;localhost:9092&quot;);
 *     props.setProperty(&quot;group.id&quot;, &quot;test&quot;);
 *     - 关闭自动提交
 *     props.setProperty(&quot;enable.auto.commit&quot;, &quot;false&quot;);
 *     props.setProperty(&quot;key.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     props.setProperty(&quot;value.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     KafkaConsumer&lt;String, String&gt; consumer = new KafkaConsumer&lt;&gt;(props);
 *     consumer.subscribe(Arrays.asList(&quot;foo&quot;, &quot;bar&quot;));
 *     final int minBatchSize = 200;
 *     List&lt;ConsumerRecord&lt;String, String&gt;&gt; buffer = new ArrayList&lt;&gt;();
 *     while (true) {
 *         ConsumerRecords&lt;String, String&gt; records = consumer.poll(Duration.ofMillis(100));
 *         - 缓存 Record，攒批
 *         for (ConsumerRecord&lt;String, String&gt; record : records) {
 *             buffer.add(record);
 *         }
 *         if (buffer.size() &gt;= minBatchSize) {
 *             - 1. 写入Db
 *             insertIntoDb(buffer);
 *             - 2. 同步提交offset
 *             consumer.commitSync();
 *             - 3. 清空buffer
 *             buffer.clear();
 *         }
 *     }
 * </pre>
 *
 * - 基于上面的场景，使用consumer.commitSync()，可以避免 ”record还没写入DB，但是已经被视为Consumed“的场景
 * In this example we will consume a batch of records and batch them up in memory. When we have enough records
 * batched, we will insert them into a database. If we allowed offsets to auto commit as in the previous example, records
 * would be considered consumed after they were returned to the user in {@link #poll(Duration) poll}. It would then be
 * possible
 * for our process to fail after batching the records, but before they had been inserted into the database.
 * <p>
 *
 *  使用consumer.commitSync()，并不能避免”有数据重复插入至DB“的问题
 *  基于此，kafka通常被认为的传递语义是 "at-least-once"
 * To avoid this, we will manually commit the offsets only after the corresponding records have been inserted into the
 * database. This gives us exact control of when a record is considered consumed. This raises the opposite possibility:
 * the process could fail in the interval after the insert into the database but before the commit (even though this
 * would likely just be a few milliseconds, it is a possibility). In this case the process that took over consumption
 * would consume from last committed offset and would repeat the insert of the last batch of data. Used in this way
 * Kafka provides what is often called "at-least-once" delivery guarantees, as each record will likely be delivered one
 * time but in failure cases could be duplicated.
 * <p>
 *
 * 使用 automatic offset commits，也是能够保证"at-least-once"的，但是需要一定的条件，就是必须一次性的把 poll 到的所有 record 全部处理掉
 * 啥意思呢？，以上面的场景为例
 * 就不能攒批了，必须把records 全部 insertIntoDb，否则 就有可能丢失数据了
 * <b>Note: Using automatic offset commits can also give you "at-least-once" delivery, but the requirement is that
 * you must consume all data returned from each call to {@link #poll(Duration)} before any subsequent calls, or before
 * {@link #close() closing} the consumer. If you fail to do either of these, it is possible for the committed offset
 * to get ahead of the consumed position, which results in missing records. The advantage of using manual offset
 * control is that you have direct control over when a record is considered "consumed."</b>
 * <p>
 *
 * - 单纯的commitSync()，会将所有的record都视作"consumed"，然后完成OffsetCommit
 * - kafka还支持更精确的 <TopicPartition - consumedOffset> 级别的提交
 * The above example uses {@link #commitSync() commitSync} to mark all received records as committed. In some cases
 * you may wish to have even finer control over which records have been committed by specifying an offset explicitly.
 * In the example below we commit offset after we finish handling the records in each partition.
 * <p>
 * <pre>
 *     try {
 *         while(running) {
 *             ConsumerRecords&lt;String, String&gt; records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
 *             for (TopicPartition partition : records.partitions()) {
 *                 List&lt;ConsumerRecord&lt;String, String&gt;&gt; partitionRecords = records.records(partition);
 *                 for (ConsumerRecord&lt;String, String&gt; record : partitionRecords) {
 *                     System.out.println(record.offset() + &quot;: &quot; + record.value());
 *                 }
 *                 long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
 *
 *                 - commitSync使用 <TopicPartition - consumedOffset> 级别的提交，注意这里为什么要 + 1
 *                 consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
 *             }
 *         }
 *     } finally {
 *       consumer.close();
 *     }
 * </pre>
 *
 * - 这里解释了为什么要加1： committed offset[want read next record offset] = now record offset + 1
 * <b>Note: The committed offset should always be the offset of the next message that your application will read.</b>
 * Thus, when calling {@link #commitSync(Map) commitSync(offsets)} you should add one to the offset of the last message processed.
 *
 * <h4><a name="manualassignment">Manual Partition Assignment</a></h4>
 *
 * - assign模式的说明， 除了 subscribe 模式，kafka还支持assign模式
 * In the previous examples, we subscribed to the topics we were interested in and let Kafka dynamically assign a
 * fair share of the partitions for those topics based on the active consumers in the group. However, in
 * some cases you may need finer control over the specific partitions that are assigned. For example:
 * <p>
 * <ul>
 * <li>If the process is maintaining some kind of local state associated with that partition (like a
 * local on-disk key-value store), then it should only get records for the partition it is maintaining on disk.
 * <li>If the process itself is highly available and will be restarted if it fails (perhaps using a
 * cluster management framework like YARN, Mesos, or AWS facilities, or as part of a stream processing framework). In
 * this case there is no need for Kafka to detect the failure and reassign the partition since the consuming process
 * will be restarted on another machine.
 * </ul>
 * <p>
 * To use this mode, instead of subscribing to the topic using {@link #subscribe(Collection) subscribe}, you just call
 * {@link #assign(Collection)} with the full list of partitions that you want to consume.
 *
 * <pre>
 *     String topic = &quot;foo&quot;;
 *     TopicPartition partition0 = new TopicPartition(topic, 0);
 *     TopicPartition partition1 = new TopicPartition(topic, 1);
 *     consumer.assign(Arrays.asList(partition0, partition1));
 * </pre>
 *
 *  - assign模式意味着 只拥有单纯的 “消费能力”，不会具备  group coordination 所支持的“扩展”能力
 * Once assigned, you can call {@link #poll(Duration) poll} in a loop, just as in the preceding examples to consume
 * records. The group that the consumer specifies is still used for committing offsets, but now the set of partitions
 * will only change with another call to {@link #assign(Collection) assign}. Manual partition assignment does
 * not use group coordination, so consumer failures will not cause assigned partitions to be rebalanced. Each consumer
 * acts independently even if it shares a groupId with another consumer. To avoid offset commit conflicts, you should
 * usually ensure that the groupId is unique for each consumer instance.
 * <p>
 *
 *  - assign模式 和 subscribe模式，无法混合使用
 * Note that it isn't possible to mix manual partition assignment (i.e. using {@link #assign(Collection) assign})
 * with dynamic partition assignment through topic subscription (i.e. using {@link #subscribe(Collection) subscribe}).
 *
 * <h4><a name="rebalancecallback">Storing Offsets Outside Kafka</h4>
 *
 *  - 并不一定是要使用 内置的__consumer_offset存储，consumer也可以选择自己存储，即 放弃 提交offset的行为
 *  - 这样做的好处就是，如果consumer能够把【存offset + 消费行为】做成一个原子化的，那就能够实现【broker - consumer】端的 "exactly once"语义了
 * The consumer application need not use Kafka's built-in offset storage, it can store offsets in a store of its own
 * choosing. The primary use case for this is allowing the application to store both the offset and the results of the
 * consumption in the same system in a way that both the results and offsets are stored atomically. This is not always
 * possible, but when it is it will make the consumption fully atomic and give "exactly once" semantics that are
 * stronger than the default "at-least once" semantics you get with Kafka's offset commit functionality.
 * <p>
 *
 * Here are a couple of examples of this type of usage:
 * <ul>
 *  exactly once 示例1： 消费的record保存在关系数据库中，在单个事务中同时保存[消费的数据 + consumed offset]
 * <li>If the results of the consumption are being stored in a relational database, storing the offset in the database
 * as well can allow committing both the results and offset in a single transaction. Thus either the transaction will
 * succeed and the offset will be updated based on what was consumed or the result will not be stored and the offset
 * won't be updated.
 *
 * exactly once 示例2：
 * 存储偏移量：当把处理结果存储到本地时，同时存储偏移量，这样可以建立结果数据与消息在分区中位置的对应关系。
 * 原子操作：以原子操作的方式存储偏移量和索引数据，能保证数据的一致性。即使系统崩溃，剩余的数据也能与对应的偏移量关联起来。
 * 避免数据丢失：在索引过程中，若因崩溃等原因丢失了近期更新，由于有偏移量的记录，索引过程可以从上次存储的偏移量位置继续进行
 * ↓
 * <li>If the results are being stored in a local store it may be possible to store the offset there as well. For
 * example a search index could be built by subscribing to a particular partition and storing both the offset and the
 * indexed data together. If this is done in a way that is atomic, it is often possible to have it be the case that even
 * if a crash occurs that causes unsync'd data to be lost, whatever is left has the corresponding offset stored as well.
 * This means that in this case the indexing process that comes back having lost recent updates just resumes indexing
 * from what it has ensuring that no updates are lost.
 * </ul>
 * <p>
 * Each record comes with its own offset, so to manage your own offset you just need to do the following:
 *
 *  自管理offset的3个基本流程：
 *  1. 存的合适
 *  2. seek(TopicPartition, long)
 *  3. 继续消费
 * <ul>
 * <li>Configure <code>enable.auto.commit=false</code>
 * <li>Use the offset provided with each {@link ConsumerRecord} to save your position.
 * <li>On restart restore the position of the consumer using {@link #seek(TopicPartition, long)}.
 * </ul>
 *
 * <p>
 * ConsumerRebalanceListener的使用场景：
 * 如果分区分配是自动完成的，则需要特别注意处理分区分配发生变化的情况，ConsumerRebalanceListener就是为了感知变化的
 * - onPartitionsRevoked: 调用时机: consumer member 放弃其所绑定的 TopicPartition时
 * - onPartitionsAssigned: 调用时机: consumer member 被分配了 新的TopicPartition后，且在FetchRequest前
 * - onPartitionsLost: 调用时机: 在异常情况下，当消费者意识到它不再拥有此分区（即未通过正常的重新平衡事件被撤销）时，则会调用此方法
 *
 * This type of usage is simplest when the partition assignment is also done manually (this would be likely in the
 * search index use case described above). If the partition assignment is done automatically special care is
 * needed to handle the case where partition assignments change. This can be done by providing a
 * {@link ConsumerRebalanceListener} instance in the call to {@link #subscribe(Collection, ConsumerRebalanceListener)}
 * and {@link #subscribe(Pattern, ConsumerRebalanceListener)}.
 * For example, when partitions are taken from a consumer the consumer will want to commit its offset for those partitions by
 * implementing {@link ConsumerRebalanceListener#onPartitionsRevoked(Collection)}. When partitions are assigned to a
 * consumer, the consumer will want to look up the offset for those new partitions and correctly initialize the consumer
 * to that position by implementing {@link ConsumerRebalanceListener#onPartitionsAssigned(Collection)}.
 * <p>
 * Another common use for {@link ConsumerRebalanceListener} is to flush any caches the application maintains for
 * partitions that are moved elsewhere.
 *
 * <h4>Controlling The Consumer's Position</h4>
 *
 * - kafka允许指定消费的位置，即可以 [重复消费已经消费过的] + [跳过消费一部分数据]
 * In most use cases the consumer will simply consume records from beginning to end, periodically committing its
 * position (either automatically or manually). However Kafka allows the consumer to manually control its position,
 * moving forward or backwards in a partition at will. This means a consumer can re-consume older records, or skip to
 * the most recent records without actually consuming the intermediate records.
 * <p>
 * There are several instances where manually controlling the consumer's position can be useful.
 * <p>
 *
 * - ”指定消费位置”之 “跳过消费”场景：对于时间敏感的Record处理，对于落后太远的消费者来说，可以直接跳到最近的Record位置处理
 * One case is for time-sensitive record processing it may make sense for a consumer that falls far enough behind to not
 * attempt to catch up processing all records, but rather just skip to the most recent records.
 * <p>
 * - ”指定消费位置”之 “反复消费”场景：某部分数据丢失了，再消费一遍即可
 * Another use case is for a system that maintains local state as described in the previous section. In such a system
 * the consumer will want to initialize its position on start-up to whatever is contained in the local store. Likewise
 * if the local state is destroyed (say because the disk is lost) the state may be recreated on a new machine by
 * re-consuming all the data and recreating the state (assuming that Kafka is retaining sufficient history).
 * <p>
 * - ”指定消费位置”的动作，是由 seek(TopicPartition, long) 完成的
 * Kafka allows specifying the position using {@link #seek(TopicPartition, long)} to specify the new position. Special
 * methods for seeking to the earliest and latest offset the server maintains are also available (
 * {@link #seekToBeginning(Collection)} and {@link #seekToEnd(Collection)} respectively).
 *
 * --------------------------------------------------------------------------------------------------->
 * <h4>Consumption Flow Control</h4>
 *
 * - 一定会存在一种情况： 1 consumer member <-> N TopicPartition
 * - 这种情况下，kafka会尽量从所有的分区中尝试获取数据，来构建FetchResponse返回
 * - 但是 还有一种情况是 consumer member想优先把某个TopicPartition的数据全部消费掉，然后再消费别的TopicPartition
 * - 如果遇到这种”需求“，该如何处理呢 ？， kafka 是否支持呢 ？
 * If a consumer is assigned multiple partitions to fetch data from, it will try to consume from all of them at the same time,
 * effectively giving these partitions the same priority for consumption. However in some cases consumers may want to
 * first focus on fetching from some subset of the assigned partitions at full speed, and only start fetching other partitions
 * when these partitions have few or no data to consume.
 *
 * <p>
 * 上面引出了，暂停TopicPartition消费的需求，下面举了2个例子
 * case1：从2个Topic消费数据，然后做join操作，如果某个Topic ”long lagging“，那么此时就会想先暂停对于另一个Topic的消费，优先把”落后太多“的Topic消费掉
 * One of such cases is stream processing, where processor fetches from two topics and performs the join on these two streams.
 * When one of the topics is long lagging behind the other, the processor would like to pause fetching from the ahead topic
 * in order to get the lagging stream to catch up.
 *
 * case2：consumer member 重启，需要消费大量历史数据，但是它想先消费掉特定的数据
 * Another example is bootstrapping upon consumer starting up where there are
 * a lot of history data to catch up, the applications usually want to get the latest data on some of the topics before consider
 * fetching other topics.
 *
 * <p>
 * 为了应对上面的需求，提供了 pause 和 resume 方法来暂停和重启 某些TopicPartition的消费
 * Kafka supports dynamic controlling of consumption flows by using {@link #pause(Collection)} and {@link #resume(Collection)}
 * to pause the consumption on the specified assigned partitions and resume the consumption
 * on the specified paused partitions respectively in the future {@link #poll(Duration)} calls.
 *
 * --------------------------------------------------------------------------------------------------->
 * <h3>Reading Transactional Messages</h3>
 * consumer member视角下的事务
 * <p>
 * 事务隔离级别配置：isolation.level=read_committed
 * Transactions were introduced in Kafka 0.11.0 wherein applications can write to multiple topics and partitions atomically.
 * In order for this to work, consumers reading from these partitions should be configured to only read committed data.
 * This can be achieved by setting the {@code isolation.level=read_committed} in the consumer's configuration.
 *
 * <p>
 * read_committed -> 只能读到已提交的事务
 * LSO -> 未完成事务的第一条消息的偏移量
 * In <code>read_committed</code> mode, the consumer will read only those transactional messages which have been
 * successfully committed. It will continue to read non-transactional messages as before. There is no client-side
 * buffering in <code>read_committed</code> mode. Instead, the end offset of a partition for a <code>read_committed</code>
 * consumer would be the offset of the first message in the partition belonging to an open transaction. This offset
 * is known as the 'Last Stable Offset'(LSO).</p>
 *
 * <p>
 * -事务性读取：只读取到LSO之前的位置 + 过滤掉废弃的事务message
 * -配置read_committed级别，会影响seekToEnd和endOffsets结果的计算
 * A {@code read_committed} consumer will only read up to the LSO and filter out any transactional
 * messages which have been aborted. The LSO also affects the behavior of {@link #seekToEnd(Collection)} and
 * {@link #endOffsets(Collection)} for {@code read_committed} consumers, details of which are in each method's documentation.
 * Finally, the fetch lag metrics are also adjusted to be relative to the LSO for {@code read_committed} consumers.
 *
 * <p>
 * - 如果TopicPartition会接受事务性消息，则其实际存储的就会包括 ”commit“和”abort“ 标志性的message
 * - 这个标志性的message，并不会返回给consumer，但是会在Log体系中拥有一个offset
 * - 所以 consumer，会发现获取的Record的offset，并不是一定连续的，gap就是 标志性message导致的
 * - 因为这类标志性message是实打实存在了”log“中，所以任何隔离级别都是有gap的
 * - 额外的 read_committed级别的consumer member， 由于其不会读取到aborted transactions对应的message，但是这些message
 *      也是实打实的存在了Log体系中，所以消费到的record，也会”Gap“
 * Partitions with transactional messages will include commit or abort markers which indicate the result of a transaction.
 * There markers are not returned to applications, yet have an offset in the log. As a result, applications reading from
 * topics with transactional messages will see gaps in the consumed offsets. These missing messages would be the transaction
 * markers, and they are filtered out for consumers in both isolation levels. Additionally, applications using
 * {@code read_committed} consumers may also see gaps due to aborted transactions, since those messages would not
 * be returned by the consumer and yet would have valid offsets.
 *
 *  --------------------------------------------------------------------------------------------------->
 *  多线程消费
 * <h3><a name="multithreaded">Multi-threaded Processing</a></h3>
 *
 * - kafka consumer 并不是线程安全的
 * The Kafka consumer is NOT thread-safe. All network I/O happens in the thread of the application
 * making the call. It is the responsibility of the user to ensure that multi-threaded access
 * is properly synchronized. Un-synchronized access will result in {@link ConcurrentModificationException}.
 *
 * <p>
 * - wakeup() 是线程安全的，可以用这个方法从另一个线程来关闭消费者
 * The only exception to this rule is {@link #wakeup()}, which can safely be used from an external thread to
 * interrupt an active operation. In this case, a {@link org.apache.kafka.common.errors.WakeupException} will be
 * thrown from the thread blocking on the operation. This can be used to shutdown the consumer from another thread.
 * The following snippet shows the typical pattern:
 *
 * <pre>
 * public class KafkaConsumerRunner implements Runnable {
 *     private final AtomicBoolean closed = new AtomicBoolean(false);
 *     private final KafkaConsumer consumer;
 *
 *     public KafkaConsumerRunner(KafkaConsumer consumer) {
 *       this.consumer = consumer;
 *     }
 *
 *     {@literal}@Override
 *     public void run() {
 *         try {
 *             consumer.subscribe(Arrays.asList("topic"));
 *             while (!closed.get()) {
 *                 ConsumerRecords records = consumer.poll(Duration.ofMillis(10000));
 *                 // Handle new records
 *             }
 *         } catch (WakeupException e) { // 上述poll流程中，如果另一线程调用了wakeup方法，则会抛出 WakeupException，以此来关闭 consumer
 *             // Ignore exception if closing
 *             if (!closed.get()) throw e;
 *         } finally {
 *             consumer.close();
 *         }
 *     }
 *
 *     // Shutdown hook which can be called from a separate thread
 *     public void shutdown() {
 *         closed.set(true);
 *         consumer.wakeup();
 *     }
 * }
 * </pre>
 *
 * Then in a separate thread, the consumer can be shutdown by setting the closed flag and waking up the consumer.
 *
 * <p>
 * <pre>
 *     closed.set(true);
 *     consumer.wakeup();
 * </pre>
 *
 * <p>
 * 除了consumer自带的wakeup方法可以达到上述目的，线程中断 interrupt()也可以做到，不过抛出的是InterruptException
 * Note that while it is possible to use thread interrupts instead of {@link #wakeup()} to abort a blocking operation
 * (in which case, {@link InterruptException} will be raised), we discourage their use since they may cause a clean
 * shutdown of the consumer to be aborted. Interrupts are mainly supported for those cases where using {@link #wakeup()}
 * is impossible, e.g. when a consumer thread is managed by code that is unaware of the Kafka client.
 *
 * <p>
 * 虽然 consumer 线程不安全的，但还是有多种 多线程 实现选项
 * We have intentionally avoided implementing a particular threading model for processing. This leaves several
 * options for implementing multi-threaded processing of records.
 *
 * <h4>1. One Consumer Per Thread</h4>
 * <模型1： 1 consumer < - > 1 Thread>
 * A simple option is to give each thread its own consumer instance. Here are the pros and cons of this approach:
 * <ul>
 * 优势
 * <li><b>PRO</b>: It is the easiest to implement [优势1： 实现简单]
 * <li><b>PRO</b>: It is often the fastest as no inter-thread co-ordination is needed [优势2： 无需考虑同步问题]
 * <li><b>PRO</b>: It makes in-order processing on a per-partition basis very easy to implement (each thread just
 * processes messages in the order it receives them). [优势3： 有利于实现 基于分区的 顺序处理]
 *
 * 劣势
 * <li><b>CON</b>: More consumers means more TCP connections to the cluster (one per thread). In general Kafka handles
 * connections very efficiently so this is generally a small cost. [劣势1：TCP连接的开销]
 * <li><b>CON</b>: Multiple consumers means more requests being sent to the server and slightly less batching of data
 * which can cause some drop in I/O throughput. [劣势2：Request数量上涨的开销]
 * <li><b>CON</b>: The number of total threads across all processes will be limited by the total number of partitions.
 * [劣势3：线程数量 受制于 分区数量]
 * </ul>
 *
 * <h4>2. Decouple Consumption and Processing</h4>
 * <模型2：分离 "消费“ 和 ”处理“ 2个流程>
 * - 1 / N consumer线程 -> [a blocking queue] -> N Process线程
 * Another alternative is to have one or more consumer threads that do all data consumption and hands off
 * {@link ConsumerRecords} instances to a blocking queue consumed by a pool of processor threads that actually handle
 * the record processing.
 *
 * This option likewise has pros and cons:
 * <ul>
 * [优势1]：可以独立的扩展 2部分的线程
 * 一种可供选择的模式是：[1 consumer 线程 + N process线程] * N
 * <li><b>PRO</b>: This option allows independently scaling the number of consumers and processors. This makes it
 * possible to have a single consumer that feeds many processor threads, avoiding any limitation on partitions.
 * [劣势1：Record 处理的顺序性很难控， 如果不需要顺序处理，可忽略]
 * <li><b>CON</b>: Guaranteeing order across the processors requires particular care as the threads will execute
 * independently an earlier chunk of data may actually be processed after a later chunk of data just due to the luck of
 * thread execution timing. For processing that has no ordering requirements this is not a problem.
 * [劣势2：何时提交 committed offset 变得更加困难]
 * <li><b>CON</b>: Manually committing the position becomes harder as it requires that all threads co-ordinate to ensure
 * that processing is complete for that partition.
 * </ul>
 *
 * 这种模型下面可以有更多的变化，
 * 比如 每个process 线程 都有一个处理的record队列， 消费者线程对消费到的Record对应的TopicPartition计算hash值，然后传给对应的process线程的队列
 * 这样可以保证某个TopicPartition的record，始终是被一个处理线程处理的，来保证分区级别的顺序处理和提交offset的可控
 * There are many possible variations on this approach. For example each processor thread can have its own queue, and
 * the consumer threads can hash into these queues using the TopicPartition to ensure in-order consumption and simplify
 * commit.
 */
public class KafkaConsumer<K, V> implements Consumer<K, V> {

    private static final long NO_CURRENT_THREAD = -1L;
    static final long DEFAULT_CLOSE_TIMEOUT_MS = 30 * 1000;
    static final String DEFAULT_REASON = "rebalance enforced by user";

    // Visible for testing
    final Metrics metrics;
	// Consumer monitoring
    final KafkaConsumerMetrics kafkaConsumerMetrics;
	private final Time time;
    private Logger log;

	// ============================ Core Components of KafkaConsumer ============================
	// clientID
    private final String clientId;
	// groupId
    private final Optional<String> groupId;

	// Consumer Coordinator
    private final ConsumerCoordinator coordinator;

	// Deserializer attach
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;

	// message fetcher: send FetchRequest and handle FetchResponse
    private final Fetcher<K, V> fetcher;
    private final OffsetFetcher offsetFetcher;
    private final TopicMetadataFetcher topicMetadataFetcher;

	// consumer interceptors
    private final ConsumerInterceptors<K, V> interceptors;

	// READ_UNCOMMITTED or READ_COMMITTED
    private final IsolationLevel isolationLevel;

	// NetworkClient
    private final ConsumerNetworkClient client;

	// Track the relationship between TopicPartition and offset
    private final SubscriptionState subscriptions;

	// ConsumerMetadata
    private final ConsumerMetadata metadata;

	// consumer allocator list: Partition allocation strategy
	private final List<ConsumerPartitionAssignor> assignors;
	// ============================ Core Components of KafkaConsumer ============================

    private final long retryBackoffMs;
    private final long requestTimeoutMs;
    private final int defaultApiTimeoutMs;
	// mark whether the consumer has closed?
    private volatile boolean closed = false;

    // currentThread holds the threadId of the current thread accessing KafkaConsumer
    // and is used to prevent multi-threaded access
    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    // refcount is used to allow reentrant access by the thread who has acquired currentThread
    private final AtomicInteger refcount = new AtomicInteger(0);

    // to keep from repeatedly scanning subscriptions in poll(), cache the result during metadata updates
    private boolean cachedSubscriptionHasAllFetchPositions;

    /**
     * A consumer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#consumerconfigs" >here</a>. Values can be
     * either strings or objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param configs The consumer configs
     */
    public KafkaConsumer(Map<String, Object> configs) {
        this(configs, null, null);
    }

    /**
     * A consumer is instantiated by providing a {@link java.util.Properties} object as configuration.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param properties The consumer configuration properties
     */
    public KafkaConsumer(Properties properties) {
        this(properties, null, null);
    }

    /**
     * A consumer is instantiated by providing a {@link java.util.Properties} object as configuration, and a
     * key and a value {@link Deserializer}.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param properties The consumer configuration properties
     * @param keyDeserializer The deserializer for key that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     * @param valueDeserializer The deserializer for value that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     */
    public KafkaConsumer(Properties properties,
                         Deserializer<K> keyDeserializer,
                         Deserializer<V> valueDeserializer) {
        this(Utils.propsToMap(properties), keyDeserializer, valueDeserializer);
    }

    /**
     * A consumer is instantiated by providing a set of key-value pairs as configuration, and a key and a value {@link Deserializer}.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param configs The consumer configs
     * @param keyDeserializer The deserializer for key that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     * @param valueDeserializer The deserializer for value that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     */
    public KafkaConsumer(Map<String, Object> configs,
                         Deserializer<K> keyDeserializer,
                         Deserializer<V> valueDeserializer) {
        this(new ConsumerConfig(ConsumerConfig.appendDeserializerToConfig(configs, keyDeserializer, valueDeserializer)),
                keyDeserializer, valueDeserializer);
    }

    @SuppressWarnings("unchecked")
    KafkaConsumer(ConsumerConfig config, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        try {
			// Group Rebalance Config
            GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(config,
                    GroupRebalanceConfig.ProtocolType.CONSUMER);
			// groupId
            this.groupId = Optional.ofNullable(groupRebalanceConfig.groupId);
			// clientId
            this.clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);
			// determine log style
            LogContext logContext = createLogContext(config, groupRebalanceConfig);
            this.log = logContext.logger(getClass());
			// enableAutoCommit?
            boolean enableAutoCommit = config.maybeOverrideEnableAutoCommit();
            groupId.ifPresent(groupIdStr -> {
                if (groupIdStr.isEmpty()) {
                    log.warn("Support for using the empty group id by consumers is deprecated and will be removed in the next major release.");
                }
            });

            log.debug("Initializing the Kafka consumer");
			// important time attach params setting
            this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG); // 30000
            this.defaultApiTimeoutMs = config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG); // 60000
	        this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG); // 100

	        // Fetch过程中的相关Metrics
            this.time = Time.SYSTEM;
            this.metrics = createMetrics(config, time);
	        FetchMetricsManager fetchMetricsManager = createFetchMetricsManager(metrics);

			// ConsumerInterceptor
            List<ConsumerInterceptor<K, V>> interceptorList = createConsumerInterceptors(config);
            this.interceptors = new ConsumerInterceptors<>(interceptorList);

			// Deserializer
            this.keyDeserializer = createKeyDeserializer(config, keyDeserializer);
            this.valueDeserializer = createValueDeserializer(config, valueDeserializer);

	        // 初始化SubscriptionState，包含后面需要的不少重要的信息：
	        // subscriptionType
	        // PartitionStates<TopicPartitionState> assignment
	        // OffsetResetStrategy
            this.subscriptions = createSubscriptionState(config, logContext);

			// clusterResourceListeners
            ClusterResourceListeners clusterResourceListeners = configureClusterResourceListeners(this.keyDeserializer,
                    this.valueDeserializer, metrics.reporters(), interceptorList);

			// *** metadata: 把上面的SubscriptionState 又包了一层
            this.metadata = new ConsumerMetadata(config, subscriptions, logContext, clusterResourceListeners);
			// broker setting
            List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config);
            this.metadata.bootstrap(addresses);

			// transaction isolation level
            this.isolationLevel = createIsolationLevel(config);

            ApiVersions apiVersions = new ApiVersions();

			// conusmerNetClient[init]：初始化 网络通信层
            this.client = createConsumerNetworkClient(config,
                    metrics,
                    logContext,
                    apiVersions,
                    time,
                    metadata,
                    fetchMetricsManager.throttleTimeSensor(),
                    retryBackoffMs);

			// Consumer partition allocation strategy，默认[ RangeAssignor + CooperativeStickyAssignor ]
            this.assignors = ConsumerPartitionAssignor.getAssignorInstances(
                    config.getList(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG),
                    config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId))
            );

            // no coordinator will be constructed for the default (null) group id
            if (!groupId.isPresent()) {
                config.ignore(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG);
                config.ignore(ConsumerConfig.THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED);
                this.coordinator = null;
            } else {
				// consumer group coordinator: 最重要的一个部分，上面的组件，都是为了封装 ConsumerCoordinator
                this.coordinator = new ConsumerCoordinator(groupRebalanceConfig,
                        logContext,
                        this.client,
                        assignors,
                        this.metadata,
                        this.subscriptions,
                        metrics,
                        CONSUMER_METRIC_GROUP_PREFIX,
                        this.time,
                        enableAutoCommit,
                        config.getInt(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG),
                        this.interceptors,
                        config.getBoolean(ConsumerConfig.THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED),
                        config.getString(ConsumerConfig.CLIENT_RACK_CONFIG));
            }

			// 3类 fetch: message + offset + topicMetaData
	        // FetchConfig{minBytes=1, maxBytes=52428800, maxWaitMs=500, fetchSize=1048576, maxPollRecords=500, ..., isolationLevel=READ_UNCOMMITTED}
            FetchConfig<K, V> fetchConfig = createFetchConfig(config, this.keyDeserializer, this.valueDeserializer);
            this.fetcher = new Fetcher<>(
                    logContext,
                    this.client,
                    this.metadata,
                    this.subscriptions,
                    fetchConfig,
                    fetchMetricsManager,
                    this.time);
			// fetch: offset
            this.offsetFetcher = new OffsetFetcher(logContext,
                    client,
                    metadata,
                    subscriptions,
                    time,
                    retryBackoffMs,
                    requestTimeoutMs,
                    isolationLevel,
                    apiVersions);
			// fetch: topicMetaData
            this.topicMetadataFetcher = new TopicMetadataFetcher(logContext, client, retryBackoffMs);

            this.kafkaConsumerMetrics = new KafkaConsumerMetrics(metrics, CONSUMER_METRIC_GROUP_PREFIX);
            config.logUnused();
            AppInfoParser.registerAppInfo(CONSUMER_JMX_PREFIX, clientId, metrics, time.milliseconds());
            log.debug("Kafka consumer initialized");
        } catch (Throwable t) {
            // call close methods if internal objects are already constructed; this is to prevent resource leak. see KAFKA-2121
            // we do not need to call `close` at all when `log` is null, which means no internal objects were initialized.
            if (this.log != null) {
                close(Duration.ZERO, true);
            }
            // now propagate the exception
            throw new KafkaException("Failed to construct kafka consumer", t);
        }
    }

    // visible for testing
    KafkaConsumer(LogContext logContext,
                  String clientId,
                  ConsumerCoordinator coordinator,
                  Deserializer<K> keyDeserializer,
                  Deserializer<V> valueDeserializer,
                  Fetcher<K, V> fetcher,
                  OffsetFetcher offsetFetcher,
                  TopicMetadataFetcher topicMetadataFetcher,
                  ConsumerInterceptors<K, V> interceptors,
                  Time time,
                  ConsumerNetworkClient client,
                  Metrics metrics,
                  SubscriptionState subscriptions,
                  ConsumerMetadata metadata,
                  long retryBackoffMs,
                  long requestTimeoutMs,
                  int defaultApiTimeoutMs,
                  List<ConsumerPartitionAssignor> assignors,
                  String groupId) {
        this.log = logContext.logger(getClass());
        this.clientId = clientId;
        this.coordinator = coordinator;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.fetcher = fetcher;
        this.offsetFetcher = offsetFetcher;
        this.topicMetadataFetcher = topicMetadataFetcher;
        this.isolationLevel = IsolationLevel.READ_UNCOMMITTED;
        this.interceptors = Objects.requireNonNull(interceptors);
        this.time = time;
        this.client = client;
        this.metrics = metrics;
        this.subscriptions = subscriptions;
        this.metadata = metadata;
        this.retryBackoffMs = retryBackoffMs;
        this.requestTimeoutMs = requestTimeoutMs;
        this.defaultApiTimeoutMs = defaultApiTimeoutMs;
        this.assignors = assignors;
        this.groupId = Optional.ofNullable(groupId);
        this.kafkaConsumerMetrics = new KafkaConsumerMetrics(metrics, "consumer");
    }

    /**
     * Get the set of partitions currently assigned to this consumer. If subscription happened by directly assigning
     * partitions using {@link #assign(Collection)} then this will simply return the same partitions that
     * were assigned. If topic subscription was used, then this will give the set of topic partitions currently assigned
     * to the consumer (which may be none if the assignment hasn't happened yet, or the partitions are in the
     * process of getting reassigned).
     * @return The set of partitions currently assigned to this consumer
     */
    public Set<TopicPartition> assignment() {
        acquireAndEnsureOpen();
        try {
            return Collections.unmodifiableSet(this.subscriptions.assignedPartitions());
        } finally {
            release();
        }
    }

    /**
     * Get the current subscription. Will return the same topics used in the most recent call to
     * {@link #subscribe(Collection, ConsumerRebalanceListener)}, or an empty set if no such call has been made.
     * @return The set of topics currently subscribed to
     */
    public Set<String> subscription() {
        acquireAndEnsureOpen();
        try {
            return Collections.unmodifiableSet(new HashSet<>(this.subscriptions.subscription()));
        } finally {
            release();
        }
    }

    /**
     * Subscribe to the given list of topics to get dynamically
     * assigned partitions. <b>Topic subscriptions are not incremental. This list will replace the current
     * assignment (if there is one).</b> Note that it is not possible to combine topic subscription with group management
     * with manual partition assignment through {@link #assign(Collection)}.
     *
     * If the given list of topics is empty, it is treated the same as {@link #unsubscribe()}.
     *
     * <p>
     * As part of group management, the consumer will keep track of the list of consumers that belong to a particular
     * group and will trigger a rebalance operation if any one of the following events are triggered:
     *
     * <ul>
     * rebalance 触发的场景： TopicPartition的变动 / consumer member的变动
     * <li>Number of partitions change for any of the subscribed topics
     * <li>A subscribed topic is created or deleted
     * <li>An existing member of the consumer group is shutdown or fails
     * <li>A new member is added to the consumer group
     * </ul>
     * <p>
     * When any of these events are triggered, the provided listener will be invoked first to indicate that
     * the consumer's assignment has been revoked, and then again when the new assignment has been received.
     *
     * consumer member 端 感知到 rebalance 并做出改变的时机
     * Note that rebalances will only occur during an active call to {@link #poll(Duration)}, so callbacks will
     * also only be invoked during that time.
     *
     * The provided listener will immediately override any listener set in a previous call to subscribe.
     * It is guaranteed, however, that the partitions revoked/assigned through this interface are from topics
     * subscribed in this call. See {@link ConsumerRebalanceListener} for more details.
     *
     * @param topics The list of topics to subscribe to
     * @param listener Non-null listener instance to get notifications on partition assignment/revocation for the
     *                 subscribed topics
     * @throws IllegalArgumentException If topics is null or contains null or empty elements, or if listener is null
     * @throws IllegalStateException If {@code subscribe()} is called previously with pattern, or assign is called
     *                               previously (without a subsequent call to {@link #unsubscribe()}), or if not
     *                               configured at-least one partition assignment strategy
     */
    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        acquireAndEnsureOpen();
        try {
			// check GroupId
            maybeThrowInvalidGroupIdException();
            if (topics == null)
                throw new IllegalArgumentException("Topic collection to subscribe to cannot be null");
            if (topics.isEmpty()) {
                // * treat subscribing to empty topic list as the same as unsubscribing
                this.unsubscribe();
            } else {
                for (String topic : topics) {
                    if (Utils.isBlank(topic))
                        throw new IllegalArgumentException("Topic collection to subscribe to cannot contain null or empty topic");
                }
				// check Assignors
                throwIfNoAssignorsConfigured();

				// Considering the situation of multiple subscribe to different topics,
	            // it is necessary to clear the data of "the topics that have been pulled but are not subscribed this time"
                fetcher.clearBufferedDataForUnassignedTopics(topics);

                log.info("Subscribed to topic(s): {}", Utils.join(topics, ", "));

				// consumer以topic名来subscribe时对SubscriptionState调用，来更新“状态”
				// Determine whether the subscribed topic needs to be updated
	            // 有个最主要的点是设置了 SubscriptionType，确定了订阅模式是 subscribe 还是 assign 模式
                if (this.subscriptions.subscribe(new HashSet<>(topics), listener))
					// 标志元数据需要更新了
					// If the topic subscribed this time is inconsistent with the last subscribed topic, the metadata needs to be updated.
                    metadata.requestUpdateForNewTopics();
            }
        } finally {
            release();
        }
    }

    /**
     * Subscribe to the given list of topics to get dynamically assigned partitions.
     * <b>Topic subscriptions are not incremental. This list will replace the current
     * assignment (if there is one).</b> It is not possible to combine topic subscription with group management
     * with manual partition assignment through {@link #assign(Collection)}.
     *
     * If the given list of topics is empty, it is treated the same as {@link #unsubscribe()}.
     *
     * <p>
     * This is a short-hand for {@link #subscribe(Collection, ConsumerRebalanceListener)}, which
     * uses a no-op listener. If you need the ability to seek to particular offsets, you should prefer
     * {@link #subscribe(Collection, ConsumerRebalanceListener)}, since group rebalances will cause partition offsets
     * to be reset. You should also provide your own listener if you are doing your own offset
     * management since the listener gives you an opportunity to commit offsets before a rebalance finishes.
     *
     * @param topics The list of topics to subscribe to
     * @throws IllegalArgumentException If topics is null or contains null or empty elements
     * @throws IllegalStateException If {@code subscribe()} is called previously with pattern, or assign is called
     *                               previously (without a subsequent call to {@link #unsubscribe()}), or if not
     *                               configured at-least one partition assignment strategy
     */
    @Override
    public void subscribe(Collection<String> topics) {
        subscribe(topics, new NoOpConsumerRebalanceListener());
    }

    /**
     * Subscribe to all topics matching specified pattern to get dynamically assigned partitions.
     * The pattern matching will be done periodically against all topics existing at the time of check.
     * This can be controlled through the {@code metadata.max.age.ms} configuration: by lowering
     * the max metadata age, the consumer will refresh metadata more often and check for matching topics.
     * <p>
     * See {@link #subscribe(Collection, ConsumerRebalanceListener)} for details on the
     * use of the {@link ConsumerRebalanceListener}. Generally rebalances are triggered when there
     * is a change to the topics matching the provided pattern and when consumer group membership changes.
     * Group rebalances only take place during an active call to {@link #poll(Duration)}.
     *
     * @param pattern Pattern to subscribe to
     * @param listener Non-null listener instance to get notifications on partition assignment/revocation for the
     *                 subscribed topics
     * @throws IllegalArgumentException If pattern or listener is null
     * @throws IllegalStateException If {@code subscribe()} is called previously with topics, or assign is called
     *                               previously (without a subsequent call to {@link #unsubscribe()}), or if not
     *                               configured at-least one partition assignment strategy
     */
    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        maybeThrowInvalidGroupIdException();
        if (pattern == null || pattern.toString().equals(""))
            throw new IllegalArgumentException("Topic pattern to subscribe to cannot be " + (pattern == null ?
                    "null" : "empty"));

        acquireAndEnsureOpen();
        try {
            throwIfNoAssignorsConfigured();
            log.info("Subscribed to pattern: '{}'", pattern);
            this.subscriptions.subscribe(pattern, listener);
            this.coordinator.updatePatternSubscription(metadata.fetch());
            this.metadata.requestUpdateForNewTopics();
        } finally {
            release();
        }
    }

    /**
     * Subscribe to all topics matching specified pattern to get dynamically assigned partitions.
     * The pattern matching will be done periodically against topics existing at the time of check.
     * <p>
     * This is a short-hand for {@link #subscribe(Pattern, ConsumerRebalanceListener)}, which
     * uses a no-op listener. If you need the ability to seek to particular offsets, you should prefer
     * {@link #subscribe(Pattern, ConsumerRebalanceListener)}, since group rebalances will cause partition offsets
     * to be reset. You should also provide your own listener if you are doing your own offset
     * management since the listener gives you an opportunity to commit offsets before a rebalance finishes.
     *
     * @param pattern Pattern to subscribe to
     * @throws IllegalArgumentException If pattern is null
     * @throws IllegalStateException If {@code subscribe()} is called previously with topics, or assign is called
     *                               previously (without a subsequent call to {@link #unsubscribe()}), or if not
     *                               configured at-least one partition assignment strategy
     */
    @Override
    public void subscribe(Pattern pattern) {
        subscribe(pattern, new NoOpConsumerRebalanceListener());
    }

    /**
     * Unsubscribe from topics currently subscribed with {@link #subscribe(Collection)} or {@link #subscribe(Pattern)}.
     * This also clears any partitions directly assigned through {@link #assign(Collection)}.
     *
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. rebalance callback errors)
     */
    public void unsubscribe() {
        acquireAndEnsureOpen();
        try {
            fetcher.clearBufferedDataForUnassignedPartitions(Collections.emptySet());
            if (this.coordinator != null) {
                this.coordinator.onLeavePrepare();
                this.coordinator.maybeLeaveGroup("the consumer unsubscribed from all topics");
            }
            this.subscriptions.unsubscribe();
            log.info("Unsubscribed all topics or patterns and assigned partitions");
        } finally {
            release();
        }
    }

    /**
     * Manually assign a list of partitions to this consumer. This interface does not allow for incremental assignment
     * and will replace the previous assignment (if there is one).
     * <p>
     * If the given list of topic partitions is empty, it is treated the same as {@link #unsubscribe()}.
     * <p>
     * Manual topic assignment through this method does not use the consumer's group management
     * functionality. As such, there will be no rebalance operation triggered when group membership or cluster and topic
     * metadata change. Note that it is not possible to use both manual partition assignment with {@link #assign(Collection)}
     * and group assignment with {@link #subscribe(Collection, ConsumerRebalanceListener)}.
     * <p>
     * 如果 auto-commit 是开启的，会在 新assignment 替换 旧assignment 之前，做一次异步提交
     * If auto-commit is enabled, an async commit (based on the old assignment) will be triggered before the new
     * assignment replaces the old one.
     *
     * @param partitions The list of partitions to assign this consumer
     * @throws IllegalArgumentException If partitions is null or contains null or empty topics
     * @throws IllegalStateException If {@code subscribe()} is called previously with topics or pattern
     *                               (without a subsequent call to {@link #unsubscribe()})
     */
    @Override
    public void assign(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
            if (partitions == null) {
                throw new IllegalArgumentException("Topic partition collection to assign to cannot be null");
            } else if (partitions.isEmpty()) {
                this.unsubscribe();
            } else {
				// 1.
                for (TopicPartition tp : partitions) {
                    String topic = (tp != null) ? tp.topic() : null;
                    if (Utils.isBlank(topic))
                        throw new IllegalArgumentException("Topic partitions to assign to cannot have null or empty topic");
                }
				// 2. 在 assign 新的之前，清除旧的 已经Fetch到的数据
                fetcher.clearBufferedDataForUnassignedPartitions(partitions);

                // 3. 如果 auto-commit 是开启的，会在 新assignment 替换 旧assignment 之前，做一次异步提交
	            // make sure the offsets of topic partitions the consumer is unsubscribing from
                // are committed since there will be no following rebalance
                if (coordinator != null)
                    this.coordinator.maybeAutoCommitOffsetsAsync(time.milliseconds());

				// 4. 更新 subscriptions 和 metadata
                log.info("Assigned to partition(s): {}", Utils.join(partitions, ", "));
				// SubscriptionState to save info and update metadata
	            // consumer以"TP"来assign时对SubscriptionState调用，来更新“状态”
                if (this.subscriptions.assignFromUser(new HashSet<>(partitions)))
					// 标志需要更新 metadata， 网络组件NetworkClient会根据标志来发送MetadataRequest
                    metadata.requestUpdateForNewTopics();
            }
        } finally {
            release();
        }
    }

    /**
     * Fetch data for the topics or partitions specified using one of the subscribe/assign APIs. It is an error to not have
     * subscribed to any topics or partitions before polling for data.
     * <p>
     * On each poll, consumer will try to use the last consumed offset as the starting offset and fetch sequentially. The last
     * consumed offset can be manually set through {@link #seek(TopicPartition, long)} or automatically set as the last committed
     * offset for the subscribed list of partitions
     *
     *
     * @param timeoutMs The time, in milliseconds, spent waiting in poll if data is not available in the buffer.
     *            If 0, returns immediately with any records that are available currently in the buffer, else returns empty.
     *            Must not be negative.
     * @return map of topic to records since the last fetch for the subscribed list of topics and partitions
     *
     * @throws org.apache.kafka.clients.consumer.InvalidOffsetException if the offset for a partition or set of
     *             partitions is undefined or out of range and no offset reset policy has been configured
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if caller lacks Read access to any of the subscribed
     *             topics or to the configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. invalid groupId or
     *             session timeout, errors deserializing key/value pairs, or any new error cases in future versions)
     * @throws java.lang.IllegalArgumentException if the timeout value is negative
     * @throws java.lang.IllegalStateException if the consumer is not subscribed to any topics or manually assigned any
     *             partitions to consume from
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance gets fenced by broker.
     *
     * @deprecated Since 2.0. Use {@link #poll(Duration)}, which does not block beyond the timeout awaiting partition
     *             assignment. See <a href="https://cwiki.apache.org/confluence/x/5kiHB">KIP-266</a> for more information.
     */
    @Deprecated
    @Override
    public ConsumerRecords<K, V> poll(final long timeoutMs) {
        return poll(time.timer(timeoutMs), false);
    }

    /**
     * Fetch data for the topics or partitions specified using one of the subscribe/assign APIs. It is an error to not have
     * subscribed to any topics or partitions before polling for data.
     * <p>
     * On each poll, consumer will try to use the last consumed offset as the starting offset and fetch sequentially. The last
     * consumed offset can be manually set through {@link #seek(TopicPartition, long)} or automatically set as the last committed
     * offset for the subscribed list of partitions
     *
     * <p>
     * This method returns immediately if there are records available or if the position advances past control records
     * or aborted transactions when isolation.level=read_committed.
     * Otherwise, it will await the passed timeout. If the timeout expires, an empty record set will be returned.
     * Note that this method may block beyond the timeout in order to execute custom
     * {@link ConsumerRebalanceListener} callbacks.
     *
     *
     * @param timeout The maximum time to block (must not be greater than {@link Long#MAX_VALUE} milliseconds)
     *
     * @return map of topic to records since the last fetch for the subscribed list of topics and partitions
     *
     * @throws org.apache.kafka.clients.consumer.InvalidOffsetException if the offset for a partition or set of
     *             partitions is undefined or out of range and no offset reset policy has been configured
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if caller lacks Read access to any of the subscribed
     *             topics or to the configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. invalid groupId or
     *             session timeout, errors deserializing key/value pairs, your rebalance callback thrown exceptions,
     *             or any new error cases in future versions)
     * @throws java.lang.IllegalArgumentException if the timeout value is negative
     * @throws java.lang.IllegalStateException if the consumer is not subscribed to any topics or manually assigned any
     *             partitions to consume from
     * @throws java.lang.ArithmeticException if the timeout is greater than {@link Long#MAX_VALUE} milliseconds.
     * @throws org.apache.kafka.common.errors.InvalidTopicException if the current subscription contains any invalid
     *             topic (per {@link org.apache.kafka.common.internals.Topic#validate(String)})
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the consumer attempts to fetch stable offsets
     *             when the broker doesn't support this feature
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance gets fenced by broker.
     *
     * 上面的ConsumerRecords<K, V> poll(final long timeoutMs)中设置了 includeMetadataInTimeout = false，但已被废弃
     * 最新的设置 includeMetadataInTimeout = true
     * includeMetadataInTimeout的设置影响了“是否在Fetch前，阻塞至，一切工作都准备完成”，最新的已经不阻塞了， KIP-266
     */
    @Override
    public ConsumerRecords<K, V> poll(final Duration timeout) {
        return poll(time.timer(timeout), true);
    }

    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    private ConsumerRecords<K, V> poll(final Timer timer, final boolean includeMetadataInTimeout) {
		//	check1: The consumer "thread is not safe", so ensure that only one thread do "poll()" for each consumer.
        acquireAndEnsureOpen();
        try {
            this.kafkaConsumerMetrics.recordPollStart(timer.currentTimeMs());
			// check2: Subscriptions Content Check，简单来说就是判断 subscriptionType == SubscriptionType.NONE
	        // 以为无论是 subscribe 还是 assign 模式，走到这里subscriptionType都不应该是 null 了
            if (this.subscriptions.hasNoSubscriptionOrUserAssignment()) {
                throw new IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions");
            }
			// 循环“pull”逻辑
            do {
				// * 如果 poll过程中，有另一个线程调用了 consumer.wakeup()方法，当前线程会感知到，并抛出 WakeupException 异常
                client.maybeTriggerWakeup();

				// *1 get partition allocation plan
	            // 每次consumer.poll()的时候都要做的：【拉取前的前置入口（保证group是"active"的 + 心跳线程的处理（sub模式下）+ 确定Fetch的位置】
                if (includeMetadataInTimeout) {
                    // try to update assignment metadata BUT do not need to block on the timer for join group
                    updateAssignmentMetadataIfNeeded(timer, false);
                } else {
					// 一直阻塞至，拉取前置完成，这还能不阻塞的吗？ -》 可以看下KIP-266
                    while (!updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE), true)) {
                        log.warn("Still waiting for metadata");
                    }
                }
				// *2 fetch by partition allocation plan | max poll 500 ConsumerRecords
                final Fetch<K, V> fetch = pollForFetches(timer);
                if (!fetch.isEmpty()) {
                    // ******* Consumer built-in optimization *******
	                // "Although I have pulled the data, I need more"
	                // before returning the fetched records, we can send off the next round of fetches
                    // and avoid block waiting for their responses to enable pipelining while the user
                    // is handling the fetched records.
                    // *******
                    // NOTE: since the consumed position has already been updated, we must not allow
                    // wakeups or any other errors to be triggered prior to returning the fetched records.
                    if (sendFetches() > 0 || client.hasPendingRequests()) {
						// will call client.poll(....)
	                    // 进入用户处理逻辑之前，如果有PendingRequests，先执行一把client.poll(timeout:0)
                        client.transmitSends();
                    }

                    if (fetch.records().isEmpty()) {
                        log.trace("Returning empty records from `poll()` "
                                + "since the consumer's position has advanced for at least one topic partition");
                    }
					// *3 **use "interceptors" to modify consumer records**
	                // 从 Fetch 往 ConsumerRecords 转, 进入用户处理逻辑
                    return this.interceptors.onConsume(new ConsumerRecords<>(fetch.records()));
                }
            } while (timer.notExpired()); //timeout is "poll(final Duration timeout)"

            return ConsumerRecords.empty();
        } finally {
            release();
            this.kafkaConsumerMetrics.recordPollEnd(timer.currentTimeMs());
        }
    }

    private int sendFetches() {
        offsetFetcher.validatePositionsOnMetadataChange();
        return fetcher.sendFetches();
    }

	/**
	 * Consumer group initialization & partition assignment
	 * 目标1：获取想订阅的 TopicPartition 的 Metadata + 成组[ subscribe模式 ]
	 * 目标2：知道从哪里开始 Fetch
	 * @param timer
	 * @param waitForJoinGroup
	 * @return
	 */
    boolean updateAssignmentMetadataIfNeeded(final Timer timer, final boolean waitForJoinGroup) {
		// 目标1. coordinator.poll: Interact with the broker side ： 保证group是"active"的 + 心跳线程的处理（sub模式下）
	    // if coordinator.poll(...) return true means "this consumer join consumer group successful
        if (coordinator != null && !coordinator.poll(timer, waitForJoinGroup)) {
            return false;
        }
		// 目标2. Set the fetch position to the committed position (if there is one) ： 确定Fetch的位置
		//    or reset it using the offset reset policy[earliest, latest] the user has configured.
        return updateFetchPositions(timer);
    }

    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    private Fetch<K, V> pollForFetches(Timer timer) {
		// 1. Calculate the waiting time for poll Min(userSetting time, heartbeat interval)
        long pollTimeout = coordinator == null ? timer.remainingMs() :
                Math.min(coordinator.timeToNextPoll(timer.currentTimeMs()), timer.remainingMs());

        // 2. if data is available already, return it immediately
        final Fetch<K, V> fetch = fetcher.collectFetch(); // ConcurrentLinkedQueue<CompletedFetch<K, V>>
        if (!fetch.isEmpty()) {
            return fetch;
        }

        // 3. send any new fetches (won't resend pending fetches), ApiKeys.FETCH
        sendFetches();

        // We do not want to be stuck blocking in poll if we are missing some positions
        // since the offset lookup may be backing off after a failure

        // NOTE: the use of cachedSubscriptionHasAllFetchPositions means we MUST call
        // updateAssignmentMetadataIfNeeded before this method.
        if (!cachedSubscriptionHasAllFetchPositions && pollTimeout > retryBackoffMs) {
            pollTimeout = retryBackoffMs;
        }

        log.trace("Polling for fetches with timeout {}", pollTimeout);

        Timer pollTimer = time.timer(pollTimeout);
		// 4. network client send request, triggering underlying NIO communication
        client.poll(pollTimer, () -> {
            // since a fetch might be completed by the background thread, we need this poll condition
            // to ensure that we do not block unnecessarily in poll()
            return !fetcher.hasAvailableFetches();
        });
        timer.update(pollTimer.currentTimeMs());
		// 5. try to get messages from cache
        return fetcher.collectFetch();
    }

    /**
     * Commit offsets returned on the last {@link #poll(Duration) poll()} for all the subscribed list of topics and
     * partitions.
     * <p>
     * This commits offsets only to Kafka. The offsets committed using this API will be used on the first fetch after
     * every rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used.
     * <p>
     * This is a synchronous commit and will block until either the commit succeeds, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout specified by {@code default.api.timeout.ms} expires
     * (in which case a {@link org.apache.kafka.common.errors.TimeoutException} is thrown to the caller).
     * <p>
     * Note that asynchronous offset commits sent previously with the {@link #commitAsync(OffsetCommitCallback)}
     * (or similar) are guaranteed to have their callbacks invoked prior to completion of this method.
     *
     * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and cannot be retried.
     *             This fatal error can only occur if you are using automatic group management with {@link #subscribe(Collection)},
     *             or if there is an active group with the same <code>group.id</code> which is using group management. In such cases,
     *             when you are trying to commit to partitions that are no longer assigned to this consumer because the
     *             consumer is for example no longer part of the group this exception would be thrown.
     * @throws org.apache.kafka.common.errors.RebalanceInProgressException if the consumer instance is in the middle of a rebalance
     *            so it is not yet determined which partitions would be assigned to the consumer. In such cases you can first
     *            complete the rebalance by calling {@link #poll(Duration)} and commit can be reconsidered afterwards.
     *            NOTE when you reconsider committing after the rebalance, the assigned partitions may have changed,
     *            and also for those partitions that are still assigned their fetch positions may have changed too
     *            if more records are returned from the {@link #poll(Duration)} call.
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. if offset metadata
     *             is too large or if the topic does not exist).
     * @throws org.apache.kafka.common.errors.TimeoutException if the timeout specified by {@code default.api.timeout.ms} expires
     *            before successful completion of the offset commit
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance gets fenced by broker.
     */
    @Override
    public void commitSync() {
        commitSync(Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * Commit offsets returned on the last {@link #poll(Duration) poll()} for all the subscribed list of topics and
     * partitions.
     * <p>
     * This commits offsets only to Kafka. The offsets committed using this API will be used on the first fetch after
     * every rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used.
     * <p>
     * This is a synchronous commit and will block until either the commit succeeds, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the passed timeout expires.
     * <p>
     * Note that asynchronous offset commits sent previously with the {@link #commitAsync(OffsetCommitCallback)}
     * (or similar) are guaranteed to have their callbacks invoked prior to completion of this method.
     *
     * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and cannot be retried.
     *             This can only occur if you are using automatic group management with {@link #subscribe(Collection)},
     *             or if there is an active group with the same <code>group.id</code> which is using group management. In such cases,
     *             when you are trying to commit to partitions that are no longer assigned to this consumer because the
     *             consumer is for example no longer part of the group this exception would be thrown.
     * @throws org.apache.kafka.common.errors.RebalanceInProgressException if the consumer instance is in the middle of a rebalance
     *            so it is not yet determined which partitions would be assigned to the consumer. In such cases you can first
     *            complete the rebalance by calling {@link #poll(Duration)} and commit can be reconsidered afterwards.
     *            NOTE when you reconsider committing after the rebalance, the assigned partitions may have changed,
     *            and also for those partitions that are still assigned their fetch positions may have changed too
     *            if more records are returned from the {@link #poll(Duration)} call.
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. if offset metadata
     *             is too large or if the topic does not exist).
     * @throws org.apache.kafka.common.errors.TimeoutException if the timeout expires before successful completion
     *            of the offset commit
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance gets fenced by broker.
     */
    @Override
    public void commitSync(Duration timeout) {
		// 从 subscriptions 中获取 Map<TopicPartition, OffsetAndMetadata>
        commitSync(subscriptions.allConsumed(), timeout);
    }

    /**
     * Commit the specified offsets for the specified list of topics and partitions.
     * <p>
     * This commits offsets to Kafka. The offsets committed using this API will be used on the first fetch after every
     * rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used. The committed offset should be the next message your application will consume,
     * i.e. lastProcessedMessageOffset + 1. If automatic group management with {@link #subscribe(Collection)} is used,
     * then the committed offsets must belong to the currently auto-assigned partitions.
     * <p>
     * This is a synchronous commit and will block until either the commit succeeds or an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout specified by {@code default.api.timeout.ms} expires
     * (in which case a {@link org.apache.kafka.common.errors.TimeoutException} is thrown to the caller).
     * <p>
     * Note that asynchronous offset commits sent previously with the {@link #commitAsync(OffsetCommitCallback)}
     * (or similar) are guaranteed to have their callbacks invoked prior to completion of this method.
     *
     * @param offsets A map of offsets by partition with associated metadata
     * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and cannot be retried.
     *             This can only occur if you are using automatic group management with {@link #subscribe(Collection)},
     *             or if there is an active group with the same <code>group.id</code> which is using group management. In such cases,
     *             when you are trying to commit to partitions that are no longer assigned to this consumer because the
     *             consumer is for example no longer part of the group this exception would be thrown.
     * @throws org.apache.kafka.common.errors.RebalanceInProgressException if the consumer instance is in the middle of a rebalance
     *            so it is not yet determined which partitions would be assigned to the consumer. In such cases you can first
     *            complete the rebalance by calling {@link #poll(Duration)} and commit can be reconsidered afterwards.
     *            NOTE when you reconsider committing after the rebalance, the assigned partitions may have changed,
     *            and also for those partitions that are still assigned their fetch positions may have changed too
     *            if more records are returned from the {@link #poll(Duration)} call, so when you retry committing
     *            you should consider updating the passed in {@code offset} parameter.
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws java.lang.IllegalArgumentException if the committed offset is negative
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. if offset metadata
     *             is too large or if the topic does not exist).
     * @throws org.apache.kafka.common.errors.TimeoutException if the timeout expires before successful completion
     *            of the offset commit
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance gets fenced by broker.
     */
    @Override
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        commitSync(offsets, Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * Commit the specified offsets for the specified list of topics and partitions.
     * <p>
     * This commits offsets to Kafka. The offsets committed using this API will be used on the first fetch after every
     * rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used. The committed offset should be the next message your application will consume,
     * i.e. lastProcessedMessageOffset + 1. If automatic group management with {@link #subscribe(Collection)} is used,
     * then the committed offsets must belong to the currently auto-assigned partitions.
     * <p>
     * This is a synchronous commit and will block until either the commit succeeds, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout expires.
     * <p>
     * Note that asynchronous offset commits sent previously with the {@link #commitAsync(OffsetCommitCallback)}
     * (or similar) are guaranteed to have their callbacks invoked prior to completion of this method.
     *
     * @param offsets A map of offsets by partition with associated metadata
     * @param timeout The maximum amount of time to await completion of the offset commit
     * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and cannot be retried.
     *             This can only occur if you are using automatic group management with {@link #subscribe(Collection)},
     *             or if there is an active group with the same <code>group.id</code> which is using group management. In such cases,
     *             when you are trying to commit to partitions that are no longer assigned to this consumer because the
     *             consumer is for example no longer part of the group this exception would be thrown.
     * @throws org.apache.kafka.common.errors.RebalanceInProgressException if the consumer instance is in the middle of a rebalance
     *            so it is not yet determined which partitions would be assigned to the consumer. In such cases you can first
     *            complete the rebalance by calling {@link #poll(Duration)} and commit can be reconsidered afterwards.
     *            NOTE when you reconsider committing after the rebalance, the assigned partitions may have changed,
     *            and also for those partitions that are still assigned their fetch positions may have changed too
     *            if more records are returned from the {@link #poll(Duration)} call, so when you retry committing
     *            you should consider updating the passed in {@code offset} parameter.
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws java.lang.IllegalArgumentException if the committed offset is negative
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. if offset metadata
     *             is too large or if the topic does not exist).
     * @throws org.apache.kafka.common.errors.TimeoutException if the timeout expires before successful completion
     *            of the offset commit
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance gets fenced by broker.
     */
    @Override
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets, final Duration timeout) {
        acquireAndEnsureOpen();
        long commitStart = time.nanoseconds();
        try {
            maybeThrowInvalidGroupIdException();
            offsets.forEach(this::updateLastSeenEpochIfNewer);
            if (!coordinator.commitOffsetsSync(new HashMap<>(offsets), time.timer(timeout))) {
                throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before successfully " +
                        "committing offsets " + offsets);
            }
        } finally {
            kafkaConsumerMetrics.recordCommitSync(time.nanoseconds() - commitStart);
            release();
        }
    }

    /**
     * Commit offsets returned on the last {@link #poll(Duration)} for all the subscribed list of topics and partition.
     * Same as {@link #commitAsync(OffsetCommitCallback) commitAsync(null)}
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance gets fenced by broker.
     */
    @Override
    public void commitAsync() {
        commitAsync(null);
    }

    /**
     * Commit offsets returned on the last {@link #poll(Duration) poll()} for the subscribed list of topics and partitions.
     * <p>
     * This commits offsets only to Kafka. The offsets committed using this API will be used on the first fetch after
     * every rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used.
     * <p>
     * This is an asynchronous call and will not block. Any errors encountered are either passed to the callback
     * (if provided) or discarded.
     * <p>
     * Offsets committed through multiple calls to this API are guaranteed to be sent in the same order as
     * the invocations. Corresponding commit callbacks are also invoked in the same order. Additionally note that
     * offsets committed through this API are guaranteed to complete before a subsequent call to {@link #commitSync()}
     * (and variants) returns.
     *
     * @param callback Callback to invoke when the commit completes
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance gets fenced by broker.
     */
    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        commitAsync(subscriptions.allConsumed(), callback);
    }

    /**
     * Commit the specified offsets for the specified list of topics and partitions to Kafka.
     * <p>
     * This commits offsets to Kafka. The offsets committed using this API will be used on the first fetch after every
     * rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used. The committed offset should be the next message your application will consume,
     * i.e. lastProcessedMessageOffset + 1. If automatic group management with {@link #subscribe(Collection)} is used,
     * then the committed offsets must belong to the currently auto-assigned partitions.
     * <p>
     * This is an asynchronous call and will not block. Any errors encountered are either passed to the callback
     * (if provided) or discarded.
     * <p>
     * Offsets committed through multiple calls to this API are guaranteed to be sent in the same order as
     * the invocations. Corresponding commit callbacks are also invoked in the same order. Additionally note that
     * offsets committed through this API are guaranteed to complete before a subsequent call to {@link #commitSync()}
     * (and variants) returns.
     *
     * @param offsets A map of offsets by partition with associate metadata. This map will be copied internally, so it
     *                is safe to mutate the map after returning.
     * @param callback Callback to invoke when the commit completes
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance gets fenced by broker.
     */
    @Override
    public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        acquireAndEnsureOpen();
        try {
            maybeThrowInvalidGroupIdException();
            log.debug("Committing offsets: {}", offsets);
            offsets.forEach(this::updateLastSeenEpochIfNewer);
            coordinator.commitOffsetsAsync(new HashMap<>(offsets), callback);
        } finally {
            release();
        }
    }

    /**
     * Overrides the fetch offsets that the consumer will use on the next {@link #poll(Duration) poll(timeout)}. If this API
     * is invoked for the same partition more than once, the latest offset will be used on the next poll(). Note that
     * you may lose data if this API is arbitrarily used in the middle of consumption, to reset the fetch offsets
     * <p>
     * The next Consumer Record which will be retrieved when poll() is invoked will have the offset specified, given that
     * a record with that offset exists (i.e. it is a valid offset).
     * <p>
     * {@link #seekToBeginning(Collection)} will go to the first offset in the topic.
     * seek(0) is equivalent to seekToBeginning for a TopicPartition with beginning offset 0,
     * assuming that there is a record at offset 0 still available.
     * {@link #seekToEnd(Collection)} is equivalent to seeking to the last offset of the partition, but behavior depends on
     * {@code isolation.level}, so see {@link #seekToEnd(Collection)} documentation for more details.
     * <p>
     * Seeking to the offset smaller than the log start offset or larger than the log end offset
     * means an invalid offset is reached.
     * Invalid offset behaviour is controlled by the {@code auto.offset.reset} property.
     * If this is set to "earliest", the next poll will return records from the starting offset.
     * If it is set to "latest", it will seek to the last offset (similar to seekToEnd()).
     * If it is set to "none", an {@code OffsetOutOfRangeException} will be thrown.
     * <p>
     * Note that, the seek offset won't change to the in-flight fetch request, it will take effect in next fetch request.
     * So, the consumer might wait for {@code fetch.max.wait.ms} before starting to fetch the records from desired offset.
     *
     * @param partition the TopicPartition on which the seek will be performed.
     * @param offset the next offset returned by poll().
     * @throws IllegalArgumentException if the provided offset is negative
     * @throws IllegalStateException if the provided TopicPartition is not assigned to this consumer
     */
    @Override
    public void seek(TopicPartition partition, long offset) {
        if (offset < 0)
            throw new IllegalArgumentException("seek offset must not be a negative number");

        acquireAndEnsureOpen();
        try {
            log.info("Seeking to offset {} for partition {}", offset, partition);
            SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                    offset,
                    Optional.empty(), // This will ensure we skip validation
                    this.metadata.currentLeader(partition));
            this.subscriptions.seekUnvalidated(partition, newPosition);
        } finally {
            release();
        }
    }

    /**
     * Overrides the fetch offsets that the consumer will use on the next {@link #poll(Duration) poll(timeout)}. If this API
     * is invoked for the same partition more than once, the latest offset will be used on the next poll(). Note that
     * you may lose data if this API is arbitrarily used in the middle of consumption, to reset the fetch offsets. This
     * method allows for setting the leaderEpoch along with the desired offset.
     *
     * @throws IllegalArgumentException if the provided offset is negative
     * @throws IllegalStateException if the provided TopicPartition is not assigned to this consumer
     */
    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        long offset = offsetAndMetadata.offset();
        if (offset < 0) {
            throw new IllegalArgumentException("seek offset must not be a negative number");
        }

        acquireAndEnsureOpen();
        try {
            if (offsetAndMetadata.leaderEpoch().isPresent()) {
                log.info("Seeking to offset {} for partition {} with epoch {}",
                        offset, partition, offsetAndMetadata.leaderEpoch().get());
            } else {
                log.info("Seeking to offset {} for partition {}", offset, partition);
            }
            Metadata.LeaderAndEpoch currentLeaderAndEpoch = this.metadata.currentLeader(partition);
            SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                    offsetAndMetadata.offset(),
                    offsetAndMetadata.leaderEpoch(),
                    currentLeaderAndEpoch);
            this.updateLastSeenEpochIfNewer(partition, offsetAndMetadata);
            this.subscriptions.seekUnvalidated(partition, newPosition);
        } finally {
            release();
        }
    }

    /**
     * Seek to the first offset for each of the given partitions. This function evaluates lazily, seeking to the
     * first offset in all partitions only when {@link #poll(Duration)} or {@link #position(TopicPartition)} are called.
     * If no partitions are provided, seek to the first offset for all of the currently assigned partitions.
     *
     * @throws IllegalArgumentException if {@code partitions} is {@code null}
     * @throws IllegalStateException if any of the provided partitions are not currently assigned to this consumer
     */
    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        if (partitions == null)
            throw new IllegalArgumentException("Partitions collection cannot be null");

        acquireAndEnsureOpen();
        try {
            Collection<TopicPartition> parts = partitions.size() == 0 ? this.subscriptions.assignedPartitions() : partitions;
            subscriptions.requestOffsetReset(parts, OffsetResetStrategy.EARLIEST);
        } finally {
            release();
        }
    }

    /**
     * Seek to the last offset for each of the given partitions. This function evaluates lazily, seeking to the
     * final offset in all partitions only when {@link #poll(Duration)} or {@link #position(TopicPartition)} are called.
     * If no partitions are provided, seek to the final offset for all of the currently assigned partitions.
     * <p>
     * If {@code isolation.level=read_committed}, the end offset will be the Last Stable Offset, i.e., the offset
     * of the first message with an open transaction.
     *
     * @throws IllegalArgumentException if {@code partitions} is {@code null}
     * @throws IllegalStateException if any of the provided partitions are not currently assigned to this consumer
     */
    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        if (partitions == null)
            throw new IllegalArgumentException("Partitions collection cannot be null");

        acquireAndEnsureOpen();
        try {
            Collection<TopicPartition> parts = partitions.size() == 0 ? this.subscriptions.assignedPartitions() : partitions;
            subscriptions.requestOffsetReset(parts, OffsetResetStrategy.LATEST);
        } finally {
            release();
        }
    }

    /**
     * Get the offset of the <i>next record</i> that will be fetched (if a record with that offset exists).
     * This method may issue a remote call to the server if there is no current position for the given partition.
     * <p>
     * This call will block until either the position could be determined or an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout specified by {@code default.api.timeout.ms} expires
     * (in which case a {@link org.apache.kafka.common.errors.TimeoutException} is thrown to the caller).
     *
     * @param partition The partition to get the position for
     * @return The current position of the consumer (that is, the offset of the next record to be fetched)
     * @throws IllegalStateException if the provided TopicPartition is not assigned to this consumer
     * @throws org.apache.kafka.clients.consumer.InvalidOffsetException if no offset is currently defined for
     *             the partition
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the consumer attempts to fetch stable offsets
     *             when the broker doesn't support this feature
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException if the position cannot be determined before the
     *             timeout specified by {@code default.api.timeout.ms} expires
     */
    @Override
    public long position(TopicPartition partition) {
        return position(partition, Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * Get the offset of the <i>next record</i> that will be fetched (if a record with that offset exists).
     * This method may issue a remote call to the server if there is no current position
     * for the given partition.
     * <p>
     * This call will block until the position can be determined, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout expires.
     *
     * @param partition The partition to get the position for
     * @param timeout The maximum amount of time to await determination of the current position
     * @return The current position of the consumer (that is, the offset of the next record to be fetched)
     * @throws IllegalStateException if the provided TopicPartition is not assigned to this consumer
     * @throws org.apache.kafka.clients.consumer.InvalidOffsetException if no offset is currently defined for
     *             the partition
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.TimeoutException if the position cannot be determined before the
     *             passed timeout expires
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     */
    @Override
    public long position(TopicPartition partition, final Duration timeout) {
        acquireAndEnsureOpen();
        try {
            if (!this.subscriptions.isAssigned(partition))
                throw new IllegalStateException("You can only check the position for partitions assigned to this consumer.");

            Timer timer = time.timer(timeout);
            do {
                SubscriptionState.FetchPosition position = this.subscriptions.validPosition(partition);
                if (position != null)
                    return position.offset;

                updateFetchPositions(timer);
                client.poll(timer);
            } while (timer.notExpired());

            throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before the position " +
                    "for partition " + partition + " could be determined");
        } finally {
            release();
        }
    }

    /**
     * Get the last committed offset for the given partition (whether the commit happened by this process or
     * another). This offset will be used as the position for the consumer in the event of a failure.
     * <p>
     * This call will do a remote call to get the latest committed offset from the server, and will block until the
     * committed offset is gotten successfully, an unrecoverable error is encountered (in which case it is thrown to
     * the caller), or the timeout specified by {@code default.api.timeout.ms} expires (in which case a
     * {@link org.apache.kafka.common.errors.TimeoutException} is thrown to the caller).
     *
     * @param partition The partition to check
     * @return The last committed offset and metadata or null if there was no prior commit
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException if the committed offset cannot be found before
     *             the timeout specified by {@code default.api.timeout.ms} expires.
     *
     * @deprecated since 2.4 Use {@link #committed(Set)} instead
     */
    @Deprecated
    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        return committed(partition, Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * Get the last committed offset for the given partition (whether the commit happened by this process or
     * another). This offset will be used as the position for the consumer in the event of a failure.
     * <p>
     * This call will block until the position can be determined, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout expires.
     *
     * @param partition The partition to check
     * @param timeout  The maximum amount of time to await the current committed offset
     * @return The last committed offset and metadata or null if there was no prior commit
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException if the committed offset cannot be found before
     *             expiration of the timeout
     *
     * @deprecated since 2.4 Use {@link #committed(Set, Duration)} instead
     */
    @Deprecated
    @Override
    public OffsetAndMetadata committed(TopicPartition partition, final Duration timeout) {
        return committed(Collections.singleton(partition), timeout).get(partition);
    }

    /**
     * Get the last committed offsets for the given partitions (whether the commit happened by this process or
     * another). The returned offsets will be used as the position for the consumer in the event of a failure.
     * <p>
     * If any of the partitions requested do not exist, an exception would be thrown.
     * <p>
     * This call will do a remote call to get the latest committed offsets from the server, and will block until the
     * committed offsets are gotten successfully, an unrecoverable error is encountered (in which case it is thrown to
     * the caller), or the timeout specified by {@code default.api.timeout.ms} expires (in which case a
     * {@link org.apache.kafka.common.errors.TimeoutException} is thrown to the caller).
     *
     * @param partitions The partitions to check
     * @return The latest committed offsets for the given partitions; {@code null} will be returned for the
     *         partition if there is no such message.
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the consumer attempts to fetch stable offsets
     *             when the broker doesn't support this feature
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException if the committed offset cannot be found before
     *             the timeout specified by {@code default.api.timeout.ms} expires.
     */
    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
        return committed(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * Get the last committed offsets for the given partitions (whether the commit happened by this process or
     * another). The returned offsets will be used as the position for the consumer in the event of a failure.
     * <p>
     * If any of the partitions requested do not exist, an exception would be thrown.
     * <p>
     * This call will block to do a remote call to get the latest committed offsets from the server.
     *
     * @param partitions The partitions to check
     * @param timeout  The maximum amount of time to await the latest committed offsets
     * @return The latest committed offsets for the given partitions; {@code null} will be returned for the
     *         partition if there is no such message.
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException if the committed offset cannot be found before
     *             expiration of the timeout
     */
    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions, final Duration timeout) {
        acquireAndEnsureOpen();
        long start = time.nanoseconds();
        try {
            maybeThrowInvalidGroupIdException();
            final Map<TopicPartition, OffsetAndMetadata> offsets;
            offsets = coordinator.fetchCommittedOffsets(partitions, time.timer(timeout));
            if (offsets == null) {
                throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before the last " +
                    "committed offset for partitions " + partitions + " could be determined. Try tuning default.api.timeout.ms " +
                    "larger to relax the threshold.");
            } else {
                offsets.forEach(this::updateLastSeenEpochIfNewer);
                return offsets;
            }
        } finally {
            kafkaConsumerMetrics.recordCommitted(time.nanoseconds() - start);
            release();
        }
    }

    /**
     * Get the metrics kept by the consumer
     */
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    /**
     * Get metadata about the partitions for a given topic. This method will issue a remote call to the server if it
     * does not already have any metadata about the given topic.
     *
     * @param topic The topic to get partition metadata for
     *
     * @return The list of partitions, which will be empty when the given topic is not found
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the specified topic. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be fetched before
     *         the amount of time allocated by {@code default.api.timeout.ms} expires.
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return partitionsFor(topic, Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * Get metadata about the partitions for a given topic. This method will issue a remote call to the server if it
     * does not already have any metadata about the given topic.
     *
     * @param topic The topic to get partition metadata for
     * @param timeout The maximum of time to await topic metadata
     *
     * @return The list of partitions, which will be empty when the given topic is not found
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the specified topic. See
     *             the exception for more details
     * @throws org.apache.kafka.common.errors.TimeoutException if topic metadata cannot be fetched before expiration
     *             of the passed timeout
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        acquireAndEnsureOpen();
        try {
            Cluster cluster = this.metadata.fetch();
            List<PartitionInfo> parts = cluster.partitionsForTopic(topic);
            if (!parts.isEmpty())
                return parts;

            Timer timer = time.timer(timeout);
            List<PartitionInfo> topicMetadata = topicMetadataFetcher.getTopicMetadata(topic, metadata.allowAutoTopicCreation(), timer);
            return topicMetadata != null ? topicMetadata : Collections.emptyList();
        } finally {
            release();
        }
    }

    /**
     * Get metadata about partitions for all topics that the user is authorized to view. This method will issue a
     * remote call to the server.

     * @return The map of topics and its partitions
     *
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be fetched before
     *         the amount of time allocated by {@code default.api.timeout.ms} expires.
     */
    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return listTopics(Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * Get metadata about partitions for all topics that the user is authorized to view. This method will issue a
     * remote call to the server.
     *
     * @param timeout The maximum time this operation will block to fetch topic metadata
     *
     * @return The map of topics and its partitions
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.TimeoutException if the topic metadata could not be fetched before
     *             expiration of the passed timeout
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     */
    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        acquireAndEnsureOpen();
        try {
            return topicMetadataFetcher.getAllTopicMetadata(time.timer(timeout));
        } finally {
            release();
        }
    }

    /**
     * Suspend fetching from the requested partitions. Future calls to {@link #poll(Duration)} will not return
     * any records from these partitions until they have been resumed using {@link #resume(Collection)}.
     * Note that this method does not affect partition subscription. In particular, it does not cause a group
     * rebalance when automatic assignment is used.
     *
     * Note: Rebalance will not preserve the pause/resume state.
     * @param partitions The partitions which should be paused
     * @throws IllegalStateException if any of the provided partitions are not currently assigned to this consumer
     */
    @Override
    public void pause(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
            log.debug("Pausing partitions {}", partitions);
            for (TopicPartition partition: partitions) {
                subscriptions.pause(partition);
            }
        } finally {
            release();
        }
    }

    /**
     * Resume specified partitions which have been paused with {@link #pause(Collection)}. New calls to
     * {@link #poll(Duration)} will return records from these partitions if there are any to be fetched.
     * If the partitions were not previously paused, this method is a no-op.
     * @param partitions The partitions which should be resumed
     * @throws IllegalStateException if any of the provided partitions are not currently assigned to this consumer
     */
    @Override
    public void resume(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
            log.debug("Resuming partitions {}", partitions);
            for (TopicPartition partition: partitions) {
                subscriptions.resume(partition);
            }
        } finally {
            release();
        }
    }

    /**
     * Get the set of partitions that were previously paused by a call to {@link #pause(Collection)}.
     *
     * @return The set of paused partitions
     */
    @Override
    public Set<TopicPartition> paused() {
        acquireAndEnsureOpen();
        try {
            return Collections.unmodifiableSet(subscriptions.pausedPartitions());
        } finally {
            release();
        }
    }

    /**
     * Look up the offsets for the given partitions by timestamp. The returned offset for each partition is the
     * earliest offset whose timestamp is greater than or equal to the given timestamp in the corresponding partition.
     *
     * This is a blocking call. The consumer does not have to be assigned the partitions.
     * If the message format version in a partition is before 0.10.0, i.e. the messages do not have timestamps, null
     * will be returned for that partition.
     *
     * @param timestampsToSearch the mapping from partition to the timestamp to look up.
     *
     * @return a mapping from partition to the timestamp and offset of the first message with timestamp greater
     *         than or equal to the target timestamp. {@code null} will be returned for the partition if there is no
     *         such message.
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic(s). See the exception for more details
     * @throws IllegalArgumentException if the target timestamp is negative
     * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be fetched before
     *         the amount of time allocated by {@code default.api.timeout.ms} expires.
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the broker does not support looking up
     *         the offsets by timestamp
     */
    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return offsetsForTimes(timestampsToSearch, Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * Look up the offsets for the given partitions by timestamp. The returned offset for each partition is the
     * earliest offset whose timestamp is greater than or equal to the given timestamp in the corresponding partition.
     *
     * This is a blocking call. The consumer does not have to be assigned the partitions.
     * If the message format version in a partition is before 0.10.0, i.e. the messages do not have timestamps, null
     * will be returned for that partition.
     *
     * @param timestampsToSearch the mapping from partition to the timestamp to look up.
     * @param timeout The maximum amount of time to await retrieval of the offsets
     *
     * @return a mapping from partition to the timestamp and offset of the first message with timestamp greater
     *         than or equal to the target timestamp. {@code null} will be returned for the partition if there is no
     *         such message.
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic(s). See the exception for more details
     * @throws IllegalArgumentException if the target timestamp is negative
     * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be fetched before
     *         expiration of the passed timeout
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the broker does not support looking up
     *         the offsets by timestamp
     */
    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        acquireAndEnsureOpen();
        try {
            for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
                // we explicitly exclude the earliest and latest offset here so the timestamp in the returned
                // OffsetAndTimestamp is always positive.
                if (entry.getValue() < 0)
                    throw new IllegalArgumentException("The target time for partition " + entry.getKey() + " is " +
                            entry.getValue() + ". The target time cannot be negative.");
            }
            return offsetFetcher.offsetsForTimes(timestampsToSearch, time.timer(timeout));
        } finally {
            release();
        }
    }

    /**
     * Get the first offset for the given partitions.
     * <p>
     * This method does not change the current consumer position of the partitions.
     *
     * @see #seekToBeginning(Collection)
     *
     * @param partitions the partitions to get the earliest offsets.
     * @return The earliest available offsets for the given partitions
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic(s). See the exception for more details
     * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be fetched before
     *         expiration of the configured {@code default.api.timeout.ms}
     */
    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return beginningOffsets(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * Get the first offset for the given partitions.
     * <p>
     * This method does not change the current consumer position of the partitions.
     *
     * @see #seekToBeginning(Collection)
     *
     * @param partitions the partitions to get the earliest offsets
     * @param timeout The maximum amount of time to await retrieval of the beginning offsets
     *
     * @return The earliest available offsets for the given partitions
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic(s). See the exception for more details
     * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be fetched before
     *         expiration of the passed timeout
     */
    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        acquireAndEnsureOpen();
        try {
            return offsetFetcher.beginningOffsets(partitions, time.timer(timeout));
        } finally {
            release();
        }
    }

    /**
     * Get the end offsets for the given partitions. In the default {@code read_uncommitted} isolation level, the end
     * offset is the high watermark (that is, the offset of the last successfully replicated message plus one). For
     * {@code read_committed} consumers, the end offset is the last stable offset (LSO), which is the minimum of
     * the high watermark and the smallest offset of any open transaction. Finally, if the partition has never been
     * written to, the end offset is 0.
     *
     * <p>
     * This method does not change the current consumer position of the partitions.
     *
     * @see #seekToEnd(Collection)
     *
     * @param partitions the partitions to get the end offsets.
     * @return The end offsets for the given partitions.
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic(s). See the exception for more details
     * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be fetched before
     *         the amount of time allocated by {@code default.api.timeout.ms} expires
     */
    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return endOffsets(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * Get the end offsets for the given partitions. In the default {@code read_uncommitted} isolation level, the end
     * offset is the high watermark (that is, the offset of the last successfully replicated message plus one). For
     * {@code read_committed} consumers, the end offset is the last stable offset (LSO), which is the minimum of
     * the high watermark and the smallest offset of any open transaction. Finally, if the partition has never been
     * written to, the end offset is 0.
     *
     * <p>
     * This method does not change the current consumer position of the partitions.
     *
     * @see #seekToEnd(Collection)
     *
     * @param partitions the partitions to get the end offsets.
     * @param timeout The maximum amount of time to await retrieval of the end offsets
     *
     * @return The end offsets for the given partitions.
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic(s). See the exception for more details
     * @throws org.apache.kafka.common.errors.TimeoutException if the offsets could not be fetched before
     *         expiration of the passed timeout
     */
    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        acquireAndEnsureOpen();
        try {
            return offsetFetcher.endOffsets(partitions, time.timer(timeout));
        } finally {
            release();
        }
    }

    /**
     * Get the consumer's current lag on the partition. Returns an "empty" {@link OptionalLong} if the lag is not known,
     * for example if there is no position yet, or if the end offset is not known yet.
     *
     * <p>
     * This method uses locally cached metadata. If the log end offset is not known yet, it triggers a request to fetch
     * the log end offset, but returns immediately.
     *
     * @param topicPartition The partition to get the lag for.
     *
     * @return This {@code Consumer} instance's current lag for the given partition.
     *
     * @throws IllegalStateException if the {@code topicPartition} is not assigned
     **/
    @Override
    public OptionalLong currentLag(TopicPartition topicPartition) {
        acquireAndEnsureOpen();
        try {
            final Long lag = subscriptions.partitionLag(topicPartition, isolationLevel);

            // if the log end offset is not known and hence cannot return lag and there is
            // no in-flight list offset requested yet,
            // issue a list offset request for that partition so that next time
            // we may get the answer; we do not need to wait for the return value
            // since we would not try to poll the network client synchronously
            if (lag == null) {
                if (subscriptions.partitionEndOffset(topicPartition, isolationLevel) == null &&
                    !subscriptions.partitionEndOffsetRequested(topicPartition)) {
                    log.info("Requesting the log end offset for {} in order to compute lag", topicPartition);
                    subscriptions.requestPartitionEndOffset(topicPartition);
                    offsetFetcher.endOffsets(Collections.singleton(topicPartition), time.timer(0L));
                }

                return OptionalLong.empty();
            }

            return OptionalLong.of(lag);
        } finally {
            release();
        }
    }

    /**
     * Return the current group metadata associated with this consumer.
     *
     * @return consumer group metadata
     * @throws org.apache.kafka.common.errors.InvalidGroupIdException if consumer does not have a group
     */
    @Override
    public ConsumerGroupMetadata groupMetadata() {
        acquireAndEnsureOpen();
        try {
            maybeThrowInvalidGroupIdException();
            return coordinator.groupMetadata();
        } finally {
            release();
        }
    }

    /**
     * Alert the consumer to trigger a new rebalance by rejoining the group. This is a nonblocking call that forces
     * the consumer to trigger a new rebalance on the next {@link #poll(Duration)} call. Note that this API does not
     * itself initiate the rebalance, so you must still call {@link #poll(Duration)}. If a rebalance is already in
     * progress this call will be a no-op. If you wish to force an additional rebalance you must complete the current
     * one by calling poll before retrying this API.
     * <p>
     * You do not need to call this during normal processing, as the consumer group will manage itself
     * automatically and rebalance when necessary. However there may be situations where the application wishes to
     * trigger a rebalance that would otherwise not occur. For example, if some condition external and invisible to
     * the Consumer and its group changes in a way that would affect the userdata encoded in the
     * {@link org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription Subscription}, the Consumer
     * will not be notified and no rebalance will occur. This API can be used to force the group to rebalance so that
     * the assignor can perform a partition reassignment based on the latest userdata. If your assignor does not use
     * this userdata, or you do not use a custom
     * {@link org.apache.kafka.clients.consumer.ConsumerPartitionAssignor ConsumerPartitionAssignor}, you should not
     * use this API.
     *
     * @param reason The reason why the new rebalance is needed.
     *
     * @throws java.lang.IllegalStateException if the consumer does not use group subscription
     */
    @Override
    public void enforceRebalance(final String reason) {
        acquireAndEnsureOpen();
        try {
            if (coordinator == null) {
                throw new IllegalStateException("Tried to force a rebalance but consumer does not have a group.");
            }
            coordinator.requestRejoin(reason == null || reason.isEmpty() ? DEFAULT_REASON : reason);
        } finally {
            release();
        }
    }

    /**
     * @see #enforceRebalance(String)
     */
    @Override
    public void enforceRebalance() {
        enforceRebalance(null);
    }

    /**
     * Close the consumer, waiting for up to the default timeout of 30 seconds for any needed cleanup.
     * If auto-commit is enabled, this will commit the current offsets if possible within the default
     * timeout. See {@link #close(Duration)} for details. Note that {@link #wakeup()}
     * cannot be used to interrupt close.
     *
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted
     *             before or while this function is called
     * @throws org.apache.kafka.common.KafkaException for any other error during close
     */
    @Override
    public void close() {
        close(Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS));
    }

    /**
     * Tries to close the consumer cleanly within the specified timeout. This method waits up to
     * {@code timeout} for the consumer to complete pending commits and leave the group.
     * If auto-commit is enabled, this will commit the current offsets if possible within the
     * timeout. If the consumer is unable to complete offset commits and gracefully leave the group
     * before the timeout expires, the consumer is force closed. Note that {@link #wakeup()} cannot be
     * used to interrupt close.
     *
     * @param timeout The maximum time to wait for consumer to close gracefully. The value must be
     *                non-negative. Specifying a timeout of zero means do not wait for pending requests to complete.
     *
     * @throws IllegalArgumentException If the {@code timeout} is negative.
     * @throws InterruptException If the thread is interrupted before or while this function is called
     * @throws org.apache.kafka.common.KafkaException for any other error during close
     */
    @Override
    public void close(Duration timeout) {
        if (timeout.toMillis() < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");
        acquire();
        try {
            if (!closed) {
                // need to close before setting the flag since the close function
                // itself may trigger rebalance callback that needs the consumer to be open still
                close(timeout, false);
            }
        } finally {
            closed = true;
            release();
        }
    }

    /**
     * Wakeup the consumer. This method is thread-safe and is useful in particular to abort a long poll.
     * The thread which is blocking in an operation will throw {@link org.apache.kafka.common.errors.WakeupException}.
     * If no thread is blocking in a method which can throw {@link org.apache.kafka.common.errors.WakeupException}, the next call to such a method will raise it instead.
     */
    @Override
    public void wakeup() {
        this.client.wakeup();
    }

    private ClusterResourceListeners configureClusterResourceListeners(Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, List<?>... candidateLists) {
        ClusterResourceListeners clusterResourceListeners = new ClusterResourceListeners();
        for (List<?> candidateList: candidateLists)
            clusterResourceListeners.maybeAddAll(candidateList);

        clusterResourceListeners.maybeAdd(keyDeserializer);
        clusterResourceListeners.maybeAdd(valueDeserializer);
        return clusterResourceListeners;
    }

    private Timer createTimerForRequest(final Duration timeout) {
        // this.time could be null if an exception occurs in constructor prior to setting the this.time field
        final Time localTime = (time == null) ? Time.SYSTEM : time;
        return localTime.timer(Math.min(timeout.toMillis(), requestTimeoutMs));
    }

    private void close(Duration timeout, boolean swallowException) {
        log.trace("Closing the Kafka consumer");
        AtomicReference<Throwable> firstException = new AtomicReference<>();

        final Timer closeTimer = createTimerForRequest(timeout);
        // Close objects with a timeout. The timeout is required because the coordinator & the fetcher send requests to
        // the server in the process of closing which may not respect the overall timeout defined for closing the
        // consumer.
        if (coordinator != null) {
            // This is a blocking call bound by the time remaining in closeTimer
            Utils.swallow(log, Level.ERROR, "Failed to close coordinator with a timeout(ms)=" + closeTimer.timeoutMs(), () -> coordinator.close(closeTimer), firstException);
        }

        if (fetcher != null) {
            // the timeout for the session close is at-most the requestTimeoutMs
            long remainingDurationInTimeout = Math.max(0, timeout.toMillis() - closeTimer.elapsedMs());
            if (remainingDurationInTimeout > 0) {
                remainingDurationInTimeout = Math.min(requestTimeoutMs, remainingDurationInTimeout);
            }

            closeTimer.reset(remainingDurationInTimeout);

            // This is a blocking call bound by the time remaining in closeTimer
            Utils.swallow(log, Level.ERROR, "Failed to close fetcher with a timeout(ms)=" + closeTimer.timeoutMs(), () -> fetcher.close(closeTimer), firstException);
        }

        Utils.closeQuietly(interceptors, "consumer interceptors", firstException);
        Utils.closeQuietly(kafkaConsumerMetrics, "kafka consumer metrics", firstException);
        Utils.closeQuietly(metrics, "consumer metrics", firstException);
        Utils.closeQuietly(client, "consumer network client", firstException);
        Utils.closeQuietly(keyDeserializer, "consumer key deserializer", firstException);
        Utils.closeQuietly(valueDeserializer, "consumer value deserializer", firstException);
        AppInfoParser.unregisterAppInfo(CONSUMER_JMX_PREFIX, clientId, metrics);
        log.debug("Kafka consumer has been closed");
        Throwable exception = firstException.get();
        if (exception != null && !swallowException) {
            if (exception instanceof InterruptException) {
                throw (InterruptException) exception;
            }
            throw new KafkaException("Failed to close kafka consumer", exception);
        }
    }

    /**
     * Set the fetch position to the committed position (if there is one)
     * or reset it using the offset reset policy the user has configured.
     *
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws NoOffsetForPartitionException If no offset is stored for a given partition and no offset reset policy is
     *             defined
     * @return true iff the operation completed without timing out
     */
    private boolean updateFetchPositions(final Timer timer) {
        // 1. If any partitions have been truncated due to a leader change, we need to validate the offsets
        offsetFetcher.validatePositionsIfNeeded();

		// 2. if subscriptions already get "Positions", return directly
	    // subscriptions中已经确定了从哪Fetch，就可以直接返回了
        cachedSubscriptionHasAllFetchPositions = subscriptions.hasAllFetchPositions();
        if (cachedSubscriptionHasAllFetchPositions) return true;

        // 3. 使用 【OffsetFetchRequest】 尝试从 broker 端的 Coordinator 来获取 Committed Offset
	    // If there are any partitions which do not have a valid position and are not
        // awaiting reset, then we need to fetch committed offsets. We will only do a
        // coordinator lookup if there are partitions which have missing positions, so
        // a consumer with manually assigned partitions can avoid a coordinator dependence
        // by always ensuring that assigned partitions have an initial position.
        if (coordinator != null && !coordinator.refreshCommittedOffsetsIfNeeded(timer)) return false;

        // 4. 也没能从Coordinator里面获取到从哪Fetch，那就根据 策略来获取 LATEST, EARLIEST
	    // If there are partitions still needing a position and a reset policy is defined,
        // request reset using the default policy. If no reset strategy is defined and there
        // are partitions with a missing position, then we will raise an exception.
        subscriptions.resetInitializingPositions();

        // 5. 以default policy(LATEST, EARLIEST), 构造 【ListOffsetRequest】结果来，最后更新了subscriptionState - TopicPartitionState - FetchPosition
	    // Finally send an asynchronous request to look up and update the positions of any
        // partitions which are awaiting reset.
        offsetFetcher.resetPositionsIfNeeded();

        return true;
    }

    /**
     * Acquire the light lock and ensure that the consumer hasn't been closed.
     * @throws IllegalStateException If the consumer has been closed
     */
    private void acquireAndEnsureOpen() {
        acquire();
        if (this.closed) {
            release();
            throw new IllegalStateException("This consumer has already been closed.");
        }
    }

    /**
     * Acquire the light lock protecting this consumer from multi-threaded access. Instead of blocking
     * when the lock is not available, however, we just throw an exception (since multi-threaded usage is not
     * supported).
     * @throws ConcurrentModificationException if another thread already has the lock
     */
    private void acquire() {
        final Thread thread = Thread.currentThread();
        final long threadId = thread.getId();
        if (threadId != currentThread.get() && !currentThread.compareAndSet(NO_CURRENT_THREAD, threadId))
            throw new ConcurrentModificationException("KafkaConsumer is not safe for multi-threaded access. " +
                    "currentThread(name: " + thread.getName() + ", id: " + threadId + ")" +
                    " otherThread(id: " + currentThread.get() + ")"
            );
        refcount.incrementAndGet();
    }

    /**
     * Release the light lock protecting the consumer from multi-threaded access.
     */
    private void release() {
        if (refcount.decrementAndGet() == 0)
            currentThread.set(NO_CURRENT_THREAD);
    }

    private void throwIfNoAssignorsConfigured() {
        if (assignors.isEmpty())
            throw new IllegalStateException("Must configure at least one partition assigner class name to " +
                ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG + " configuration property");
    }

    private void maybeThrowInvalidGroupIdException() {
        if (!groupId.isPresent())
            throw new InvalidGroupIdException("To use the group management or offset commit APIs, you must " +
                    "provide a valid " + ConsumerConfig.GROUP_ID_CONFIG + " in the consumer configuration.");
    }

    private void updateLastSeenEpochIfNewer(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata) {
        if (offsetAndMetadata != null)
            offsetAndMetadata.leaderEpoch().ifPresent(epoch -> metadata.updateLastSeenEpochIfNewer(topicPartition, epoch));
    }

    // Functions below are for testing only
    String getClientId() {
        return clientId;
    }

    boolean updateAssignmentMetadataIfNeeded(final Timer timer) {
        return updateAssignmentMetadataIfNeeded(timer, true);
    }
}
