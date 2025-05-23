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

import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import org.slf4j.Logger;

/**
 * A helper class for managing the heartbeat to the coordinator
 */
public final class Heartbeat {
    private final int maxPollIntervalMs;
    private final GroupRebalanceConfig rebalanceConfig;
    private final Time time;
    private final Timer heartbeatTimer;
    private final Timer sessionTimer;
    private final Timer pollTimer;
    private final Logger log;

    private volatile long lastHeartbeatSend = 0L;
    private volatile boolean heartbeatInFlight = false;

    public Heartbeat(GroupRebalanceConfig config,
                     Time time) {
        if (config.heartbeatIntervalMs >= config.sessionTimeoutMs)
            throw new IllegalArgumentException("Heartbeat must be set lower than the session timeout");
        this.rebalanceConfig = config;
        this.time = time;
        this.heartbeatTimer = time.timer(config.heartbeatIntervalMs);
        this.sessionTimer = time.timer(config.sessionTimeoutMs);
        this.maxPollIntervalMs = config.rebalanceTimeoutMs;
        this.pollTimer = time.timer(maxPollIntervalMs);

        final LogContext logContext = new LogContext("[Heartbeat groupID=" + config.groupId + "] ");
        this.log = logContext.logger(getClass());
    }

    private void update(long now) {
        heartbeatTimer.update(now);
        sessionTimer.update(now);
        pollTimer.update(now);
    }

    public void poll(long now) {
        update(now);
        pollTimer.reset(maxPollIntervalMs);
    }

    boolean hasInflight() {
        return heartbeatInFlight;
    }

    void sentHeartbeat(long now) {
        lastHeartbeatSend = now;
        heartbeatInFlight = true;
        update(now);
        // 发送心跳请求之后，更新heartbeatIntervalMs。标志下次发送的时间应该是更新heartbeatIntervalMs之后
        heartbeatTimer.reset(rebalanceConfig.heartbeatIntervalMs);

        if (log.isTraceEnabled()) {
            log.trace("Sending heartbeat request with {}ms remaining on timer", heartbeatTimer.remainingMs());
        }
    }

    void failHeartbeat() {
        update(time.milliseconds());
        heartbeatInFlight = false;
        // 收到心跳响应->失败之后，下次发送的时间应该是retryBackoffMs之后
        heartbeatTimer.reset(rebalanceConfig.retryBackoffMs);

        log.trace("Heartbeat failed, reset the timer to {}ms remaining", heartbeatTimer.remainingMs());
    }

    void receiveHeartbeat() {
        update(time.milliseconds());
        heartbeatInFlight = false;
        // 收到心跳响应->成功之后，更新sessionTimeoutMs，标志这个session内是正常的
        sessionTimer.reset(rebalanceConfig.sessionTimeoutMs);
    }

    boolean shouldHeartbeat(long now) {
        update(now); //更新相关时间为now，相当于推进时间
        return heartbeatTimer.isExpired(); // 判断是否expire
    }
    
    long lastHeartbeatSend() {
        return this.lastHeartbeatSend;
    }

    long timeToNextHeartbeat(long now) {
        update(now);
        return heartbeatTimer.remainingMs();
    }

    boolean sessionTimeoutExpired(long now) {
        update(now);
        return sessionTimer.isExpired();
    }

    void resetTimeouts() {
        update(time.milliseconds());
        sessionTimer.reset(rebalanceConfig.sessionTimeoutMs);
        pollTimer.reset(maxPollIntervalMs);
        heartbeatTimer.reset(rebalanceConfig.heartbeatIntervalMs);
    }

    void resetSessionTimeout() {
        update(time.milliseconds());
        sessionTimer.reset(rebalanceConfig.sessionTimeoutMs);
    }

    boolean pollTimeoutExpired(long now) {
        update(now);
        return pollTimer.isExpired();
    }

    long lastPollTime() {
        return pollTimer.currentTimeMs();
    }
}
