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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class manages the fetching process with the brokers.
 * <p>
 * Thread-safety:
 * Requests and responses of Fetcher may be processed by different threads since heartbeat
 * thread may process responses. Other operations are single-threaded and invoked only from
 * the thread polling the consumer.
 * <ul>
 *     <li>If a response handler accesses any shared state of the Fetcher (e.g. FetchSessionHandler),
 *     all access to that state must be synchronized on the Fetcher instance.</li>
 *     <li>If a response handler accesses any shared state of the coordinator (e.g. SubscriptionState),
 *     it is assumed that all access to that state is synchronized on the coordinator instance by
 *     the caller.</li>
 *     <li>At most one request is pending for each node at any time. Nodes with pending requests are
 *     tracked and updated after processing the response. This ensures that any state (e.g. epoch)
 *     updated while processing responses on one thread are visible while creating the subsequent request
 *     on a different thread.</li>
 * </ul>
 */
public class Fetcher<K, V> extends AbstractFetch<K, V> {

    private final Logger log;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public Fetcher(LogContext logContext,
                   ConsumerNetworkClient client,
                   ConsumerMetadata metadata,
                   SubscriptionState subscriptions,
                   FetchConfig<K, V> fetchConfig,
                   FetchMetricsManager metricsManager,
                   Time time) {
        super(logContext, client, metadata, subscriptions, fetchConfig, metricsManager, time);
        this.log = logContext.logger(Fetcher.class);
    }

    /**
     * Set-up a fetch request for any node that we have assigned partitions for which doesn't already have
     * an in-flight fetch or pending fetch data.
     * 1. 构建 FetchRequest
     * 2. 预发送
     * 3. 定义了 FetchResponse 成功处理时的回调： handleFetchResponse
     * @return number of fetches sent
     */
    public synchronized int sendFetches() {
        // 1. prepare FetchRequests: <Node, FetchRequestData>
        Map<Node, FetchSessionHandler.FetchRequestData> fetchRequestMap = prepareFetchRequests();
        // 2. traverse with node
        for (Map.Entry<Node, FetchSessionHandler.FetchRequestData> entry : fetchRequestMap.entrySet()) {
            // 2.1. get target node
            final Node fetchTarget = entry.getKey();
            // 2.2. prepare FetchRequestData
            final FetchSessionHandler.FetchRequestData data = entry.getValue();
            // 2.3 prepare FetchRequest
            final FetchRequest.Builder request = createFetchRequest(fetchTarget, data);
            // 2.4 RequestFutureListener
            RequestFutureListener<ClientResponse> listener = new RequestFutureListener<ClientResponse>() {
                @Override
                public void onSuccess(ClientResponse resp) {
                    synchronized (Fetcher.this) {
                        // Implements the core logic for a successful fetch request/response.
                        // put "records" to completedFetches | [ completedFetches.add(completedFetch) ]
                        handleFetchResponse(fetchTarget, data, resp);
                    }
                }

                @Override
                public void onFailure(RuntimeException e) {
                    synchronized (Fetcher.this) {
                        handleFetchResponse(fetchTarget, e);
                    }
                }
            };
            // 2.5 true send ApiKeys.FETCH to "Send Buffer"
            final RequestFuture<ClientResponse> future = client.send(fetchTarget, request);
            future.addListener(listener);
        }

        return fetchRequestMap.size();
    }

    public void close(final Timer timer) {
        if (!isClosed.compareAndSet(false, true)) {
            log.info("Fetcher {} is already closed.", this);
            return;
        }

        // Shared states (e.g. sessionHandlers) could be accessed by multiple threads (such as heartbeat thread), hence,
        // it is necessary to acquire a lock on the fetcher instance before modifying the states.
        synchronized (this) {
            super.close(timer);
        }
    }
}
