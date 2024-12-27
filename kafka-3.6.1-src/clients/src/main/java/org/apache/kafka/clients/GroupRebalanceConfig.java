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
package org.apache.kafka.clients;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.requests.JoinGroupRequest;

import java.util.Locale;
import java.util.Optional;

/**
 * Class to extract group rebalance related configs.
 */
public class GroupRebalanceConfig {

    public enum ProtocolType {
        CONSUMER,
        CONNECT;

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }
    }
    // 7个重要的属性
    public final int sessionTimeoutMs; // session.timeout.ms: 45000 : How long does it take for the consumer and broker to have no heartbeat to trigger rebalance
    public final int rebalanceTimeoutMs; // max.poll.interval.ms: 300000
    public final int heartbeatIntervalMs; // heartbeat.interval.ms: 3000
    public final String groupId;
    public final Optional<String> groupInstanceId;
    public final long retryBackoffMs;
    public final boolean leaveGroupOnClose;

    public GroupRebalanceConfig(AbstractConfig config, ProtocolType protocolType) {
        // 1. 心跳响应<->超时时间设置
        this.sessionTimeoutMs = config.getInt(CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG);

        // Consumer and Connect use different config names for defining rebalance timeout
        // 2. 拉取间隔<->超时时间设置
        if (protocolType == ProtocolType.CONSUMER) {
            this.rebalanceTimeoutMs = config.getInt(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG);
        } else {
            this.rebalanceTimeoutMs = config.getInt(CommonClientConfigs.REBALANCE_TIMEOUT_MS_CONFIG);
        }
        // 3. 心跳发送<->时间间隔设置
        this.heartbeatIntervalMs = config.getInt(CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG);

        // 4. 消费者组groupID设置
        this.groupId = config.getString(CommonClientConfigs.GROUP_ID_CONFIG);

        // 5. Static membership is only introduced in consumer API.
        // 静态消费者的标志：group.instance.id
        if (protocolType == ProtocolType.CONSUMER) {
            String groupInstanceId = config.getString(CommonClientConfigs.GROUP_INSTANCE_ID_CONFIG);
            if (groupInstanceId != null) {
                JoinGroupRequest.validateGroupInstanceId(groupInstanceId);
                this.groupInstanceId = Optional.of(groupInstanceId);
            } else {
                this.groupInstanceId = Optional.empty();
            }
        } else {
            this.groupInstanceId = Optional.empty();
        }
        // 6. 失败回退时间
        this.retryBackoffMs = config.getLong(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG);

        // Internal leave group config is only defined in Consumer.
        // 7. consumer退出时的行为：默认是true
        if (protocolType == ProtocolType.CONSUMER) {
            this.leaveGroupOnClose = config.getBoolean("internal.leave.group.on.close");
        } else {
            this.leaveGroupOnClose = true;
        }
    }

    // For testing purpose.
    public GroupRebalanceConfig(final int sessionTimeoutMs,
                                final int rebalanceTimeoutMs,
                                final int heartbeatIntervalMs,
                                String groupId,
                                Optional<String> groupInstanceId,
                                long retryBackoffMs,
                                boolean leaveGroupOnClose) {
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.groupId = groupId;
        this.groupInstanceId = groupInstanceId;
        this.retryBackoffMs = retryBackoffMs;
        this.leaveGroupOnClose = leaveGroupOnClose;
    }
}
