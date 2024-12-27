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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ProducerMetadata extends Metadata {
    // If a topic hasn't been accessed for this many milliseconds, it is removed from the cache.
    private final long metadataIdleMs;

    /* Topics(already get metadata) with expiry time [topic, nowMs + metadataIdleMs] */
    private final Map<String, Long> topics = new HashMap<>();
    /* Topics(want to get metadata) */
    private final Set<String> newTopics = new HashSet<>();
    private final Logger log;
    private final Time time;

    public ProducerMetadata(long refreshBackoffMs,
                            long metadataExpireMs,
                            long metadataIdleMs,
                            LogContext logContext,
                            ClusterResourceListeners clusterResourceListeners,
                            Time time) {
        // call Metadata init
        super(refreshBackoffMs, metadataExpireMs, logContext, clusterResourceListeners);
        this.metadataIdleMs = metadataIdleMs;
        this.log = logContext.logger(ProducerMetadata.class);
        this.time = time;
    }

    @Override
    public synchronized MetadataRequest.Builder newMetadataRequestBuilder() {
        return new MetadataRequest.Builder(new ArrayList<>(topics.keySet()), true);
    }

    @Override
    public synchronized MetadataRequest.Builder newMetadataRequestBuilderForNewTopics() {
        return new MetadataRequest.Builder(new ArrayList<>(newTopics), true);
    }

    public synchronized void add(String topic, long nowMs) {
        Objects.requireNonNull(topic, "topic cannot be null");
        // A null return indicate that the map previously associated null with key, means new topic add
        if (topics.put(topic, nowMs + metadataIdleMs) == null) {
            newTopics.add(topic);
            // mark need update
            requestUpdateForNewTopics();
        }
    }

    /**
     * judge Partial update or Full update
     * @param topic
     * @return
     */
    public synchronized int requestUpdateForTopic(String topic) {
        if (newTopics.contains(topic)) {
            // Partial update
            return requestUpdateForNewTopics();
        } else {
            // Full update
            return requestUpdate();
        }
    }

    // Visible for testing
    synchronized Set<String> topics() {
        return topics.keySet();
    }

    // Visible for testing
    synchronized Set<String> newTopics() {
        return newTopics;
    }

    public synchronized boolean containsTopic(String topic) {
        return topics.containsKey(topic);
    }

    /**
     * Determine whether the metadata of this topic should be retained
     * @param topic
     * @param isInternal
     * @param nowMs
     * @return true/false
     */
    @Override
    public synchronized boolean retainTopic(String topic, boolean isInternal, long nowMs) {
        // 1. get expireMs = "nowMs + metadataIdleMs", define in add(....)
        Long expireMs = topics.get(topic);
        if (expireMs == null) {
            return false;
        // 2. newTopic handle
        } else if (newTopics.contains(topic)) {
            return true;
        // 3. topic metadata expire, do remove and return false
        } else if (expireMs <= nowMs) {
            log.debug("Removing unused topic {} from the metadata list, expiryMs {} now {}", topic, expireMs, nowMs);
            // 会出现移除了这个topic，但后续还需要的场景吗？如何处理？ => doSend时都会把需要的record的topic重新放进去
            topics.remove(topic);
            return false;
        } else {
            return true;
        }
    }

    /**
     * Wait for metadata update until the current version is larger than the last version we know of
     * @param lastVersion: last time updateVersion
     * @param timeoutMs
     * @throws InterruptedException
     */
    public synchronized void awaitUpdate(final int lastVersion, final long timeoutMs) throws InterruptedException {
        long currentTimeMs = time.milliseconds();
        long deadlineMs = currentTimeMs + timeoutMs < 0 ? Long.MAX_VALUE : currentTimeMs + timeoutMs;
        // updateVersion() will return [now updateVersion]
        // next method update() will "updateVersion += 1", so updateVersion() > lastVersion means "update metadata successfully"
        // main线程和sender线程通过wait(), notify()交互合作（通过共同持有producermetadata）
        // main会阻塞直至：updateVersion() > lastVersion, updateVersion()中所返回的updateVersion会在update()中被更新
        time.waitObject(this, () -> {
            // Throw fatal exceptions, if there are any. Recoverable topic errors will be handled by the caller.
            maybeThrowFatalException();
            return updateVersion() > lastVersion || isClosed();
        }, deadlineMs);

        if (isClosed())
            throw new KafkaException("Requested metadata update after close");
    }

    /**
     *
     * @param requestVersion The request version corresponding to the update response, as provided by
     *     {@link #newMetadataRequestAndVersion(long)}.
     * @param response metadata response received from the broker
     * @param isPartialUpdate whether the metadata request was for a subset of the active topics
     * @param nowMs current time in milliseconds
     */
    @Override
    public synchronized void update(int requestVersion, MetadataResponse response, boolean isPartialUpdate, long nowMs) {
        // 1.
        super.update(requestVersion, response, isPartialUpdate, nowMs);
        // 2.
        // Remove all topics in the response that are in the new topic set. Note that if an error was encountered for a
        // new topic's metadata, then any work to resolve the error will include the topic in a full metadata update.
        if (!newTopics.isEmpty()) {
            // If get metadata successfully, remove related topics
            for (MetadataResponse.TopicMetadata metadata : response.topicMetadata()) {
                newTopics.remove(metadata.topic());
            }
        }
        // 3. notify awaitUpdate
        notifyAll(); // 唤醒了谁？，哪个线程在哪被阻塞了 =》 唤醒main， main在awaitUpdate中被阻塞了
    }

    @Override
    public synchronized void fatalError(KafkaException fatalException) {
        super.fatalError(fatalException);
        notifyAll();
    }

    /**
     * Close this instance and notify any awaiting threads.
     */
    @Override
    public synchronized void close() {
        super.close();
        notifyAll();
    }

}
