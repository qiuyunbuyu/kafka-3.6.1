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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.SendBuilder;

import java.nio.ByteBuffer;
import java.util.Map;

public abstract class AbstractRequest implements AbstractRequestResponse {

    public static abstract class Builder<T extends AbstractRequest> {
        private final ApiKeys apiKey;
        private final short oldestAllowedVersion;
        private final short latestAllowedVersion;

        /**
         * Construct a new builder which allows any supported version
         */
        public Builder(ApiKeys apiKey, boolean enableUnstableLastVersion) {
            this(apiKey, apiKey.oldestVersion(), apiKey.latestVersion(enableUnstableLastVersion));
        }

        /**
         * Construct a new builder which allows any supported and released version
         */
        public Builder(ApiKeys apiKey) {
            this(apiKey, false);
        }

        /**
         * Construct a new builder which allows only a specific version
         */
        public Builder(ApiKeys apiKey, short allowedVersion) {
            this(apiKey, allowedVersion, allowedVersion);
        }

        /**
         * Construct a new builder which allows an inclusive range of versions
         */
        public Builder(ApiKeys apiKey, short oldestAllowedVersion, short latestAllowedVersion) {
            this.apiKey = apiKey;
            this.oldestAllowedVersion = oldestAllowedVersion;
            this.latestAllowedVersion = latestAllowedVersion;
        }

        public ApiKeys apiKey() {
            return apiKey;
        }

        public short oldestAllowedVersion() {
            return oldestAllowedVersion;
        }

        public short latestAllowedVersion() {
            return latestAllowedVersion;
        }

        public T build() {
            return build(latestAllowedVersion());
        }

        public abstract T build(short version);
    }

    private final short version;
    private final ApiKeys apiKey;

    public AbstractRequest(ApiKeys apiKey, short version) {
        if (!apiKey.isVersionSupported(version))
            throw new UnsupportedVersionException("The " + apiKey + " protocol does not support version " + version);
        this.version = version;
        this.apiKey = apiKey;
    }

    /**
     * Get the version of this AbstractRequest object.
     */
    public short version() {
        return version;
    }

    public ApiKeys apiKey() {
        return apiKey;
    }

    public final Send toSend(RequestHeader header) {
        return SendBuilder.buildRequestSend(header, data());
    }

    /**
     * Serializes header and body without prefixing with size (unlike `toSend`, which does include a size prefix).
     */
    public final ByteBuffer serializeWithHeader(RequestHeader header) {
        if (header.apiKey() != apiKey) {
            throw new IllegalArgumentException("Could not build request " + apiKey + " with header api key " + header.apiKey());
        }
        if (header.apiVersion() != version) {
            throw new IllegalArgumentException("Could not build request version " + version + " with header version " + header.apiVersion());
        }
        return RequestUtils.serialize(header.data(), header.headerVersion(), data(), version);
    }

    // Visible for testing
    public final ByteBuffer serialize() {
        return MessageUtil.toByteBuffer(data(), version);
    }

    // Visible for testing
    final int sizeInBytes() {
        return data().size(new ObjectSerializationCache(), version);
    }

    public String toString(boolean verbose) {
        return data().toString();
    }

    @Override
    public final String toString() {
        return toString(true);
    }

    /**
     * Get an error response for a request
     */
    public AbstractResponse getErrorResponse(Throwable e) {
        return getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, e);
    }

    /**
     * Get an error response for a request with specified throttle time in the response if applicable
     */
    public abstract AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e);

    /**
     * Get the error counts corresponding to an error response. This is overridden for requests
     * where response may be null (e.g produce with acks=0).
     */
    public Map<Errors, Integer> errorCounts(Throwable e) {
        AbstractResponse response = getErrorResponse(0, e);
        if (response == null)
            throw new IllegalStateException("Error counts could not be obtained for request " + this);
        else
            return response.errorCounts();
    }

    /**
     * Factory method for getting a request object based on ApiKey ID and a version
     */
    public static RequestAndSize parseRequest(ApiKeys apiKey, short apiVersion, ByteBuffer buffer) {
        // 前面读此Buffer的Header的时候，已经更新了位移，此时剩余的就是Body的大小了
        int bufferSize = buffer.remaining();
        // 解析Request
        return new RequestAndSize(doParseRequest(apiKey, apiVersion, buffer), bufferSize);
    }

    /**
     * 每个Request都实现了一套相对从Byte到Request的解析
     * @param apiKey: 确定是哪个Request
     * @param apiVersion： 确定是Request的哪个版本
     * @param buffer： Request对应的Byte
     * @return 解析好的Request
     */
    private static AbstractRequest doParseRequest(ApiKeys apiKey, short apiVersion, ByteBuffer buffer) {
        switch (apiKey) {
            case PRODUCE:
                return ProduceRequest.parse(buffer, apiVersion);
            case FETCH:
                return FetchRequest.parse(buffer, apiVersion);
            case LIST_OFFSETS:
                return ListOffsetsRequest.parse(buffer, apiVersion);
            case METADATA:
                return MetadataRequest.parse(buffer, apiVersion);
            case OFFSET_COMMIT:
                return OffsetCommitRequest.parse(buffer, apiVersion);
            case OFFSET_FETCH:
                return OffsetFetchRequest.parse(buffer, apiVersion);
            case FIND_COORDINATOR:
                return FindCoordinatorRequest.parse(buffer, apiVersion);
            case JOIN_GROUP:
                return JoinGroupRequest.parse(buffer, apiVersion);
            case HEARTBEAT:
                return HeartbeatRequest.parse(buffer, apiVersion);
            case LEAVE_GROUP:
                return LeaveGroupRequest.parse(buffer, apiVersion);
            case SYNC_GROUP:
                return SyncGroupRequest.parse(buffer, apiVersion);
            // controller Request
            case STOP_REPLICA:
                return StopReplicaRequest.parse(buffer, apiVersion);
            case CONTROLLED_SHUTDOWN:
                return ControlledShutdownRequest.parse(buffer, apiVersion);
            // controller Request
            case UPDATE_METADATA:
                return UpdateMetadataRequest.parse(buffer, apiVersion);
            // controller Request
            case LEADER_AND_ISR:
                return LeaderAndIsrRequest.parse(buffer, apiVersion);
            case DESCRIBE_GROUPS:
                return DescribeGroupsRequest.parse(buffer, apiVersion);
            case LIST_GROUPS:
                return ListGroupsRequest.parse(buffer, apiVersion);
            case SASL_HANDSHAKE:
                return SaslHandshakeRequest.parse(buffer, apiVersion);
            case API_VERSIONS:
                return ApiVersionsRequest.parse(buffer, apiVersion);
            case CREATE_TOPICS:
                return CreateTopicsRequest.parse(buffer, apiVersion);
            case DELETE_TOPICS:
                return DeleteTopicsRequest.parse(buffer, apiVersion);
            case DELETE_RECORDS:
                return DeleteRecordsRequest.parse(buffer, apiVersion);
            case INIT_PRODUCER_ID:
                return InitProducerIdRequest.parse(buffer, apiVersion);
            case OFFSET_FOR_LEADER_EPOCH:
                return OffsetsForLeaderEpochRequest.parse(buffer, apiVersion);
            case ADD_PARTITIONS_TO_TXN:
                return AddPartitionsToTxnRequest.parse(buffer, apiVersion);
            case ADD_OFFSETS_TO_TXN:
                return AddOffsetsToTxnRequest.parse(buffer, apiVersion);
            case END_TXN:
                return EndTxnRequest.parse(buffer, apiVersion);
            case WRITE_TXN_MARKERS:
                return WriteTxnMarkersRequest.parse(buffer, apiVersion);
            case TXN_OFFSET_COMMIT:
                return TxnOffsetCommitRequest.parse(buffer, apiVersion);
            case DESCRIBE_ACLS:
                return DescribeAclsRequest.parse(buffer, apiVersion);
            case CREATE_ACLS:
                return CreateAclsRequest.parse(buffer, apiVersion);
            case DELETE_ACLS:
                return DeleteAclsRequest.parse(buffer, apiVersion);
            case DESCRIBE_CONFIGS:
                return DescribeConfigsRequest.parse(buffer, apiVersion);
            case ALTER_CONFIGS:
                return AlterConfigsRequest.parse(buffer, apiVersion);
            case ALTER_REPLICA_LOG_DIRS:
                return AlterReplicaLogDirsRequest.parse(buffer, apiVersion);
            case DESCRIBE_LOG_DIRS:
                return DescribeLogDirsRequest.parse(buffer, apiVersion);
            case SASL_AUTHENTICATE:
                return SaslAuthenticateRequest.parse(buffer, apiVersion);
            case CREATE_PARTITIONS:
                return CreatePartitionsRequest.parse(buffer, apiVersion);
            case CREATE_DELEGATION_TOKEN:
                return CreateDelegationTokenRequest.parse(buffer, apiVersion);
            case RENEW_DELEGATION_TOKEN:
                return RenewDelegationTokenRequest.parse(buffer, apiVersion);
            case EXPIRE_DELEGATION_TOKEN:
                return ExpireDelegationTokenRequest.parse(buffer, apiVersion);
            case DESCRIBE_DELEGATION_TOKEN:
                return DescribeDelegationTokenRequest.parse(buffer, apiVersion);
            case DELETE_GROUPS:
                return DeleteGroupsRequest.parse(buffer, apiVersion);
            case ELECT_LEADERS:
                return ElectLeadersRequest.parse(buffer, apiVersion);
            case INCREMENTAL_ALTER_CONFIGS:
                return IncrementalAlterConfigsRequest.parse(buffer, apiVersion);
            case ALTER_PARTITION_REASSIGNMENTS:
                return AlterPartitionReassignmentsRequest.parse(buffer, apiVersion);
            case LIST_PARTITION_REASSIGNMENTS:
                return ListPartitionReassignmentsRequest.parse(buffer, apiVersion);
            case OFFSET_DELETE:
                return OffsetDeleteRequest.parse(buffer, apiVersion);
            case DESCRIBE_CLIENT_QUOTAS:
                return DescribeClientQuotasRequest.parse(buffer, apiVersion);
            case ALTER_CLIENT_QUOTAS:
                return AlterClientQuotasRequest.parse(buffer, apiVersion);
            case DESCRIBE_USER_SCRAM_CREDENTIALS:
                return DescribeUserScramCredentialsRequest.parse(buffer, apiVersion);
            case ALTER_USER_SCRAM_CREDENTIALS:
                return AlterUserScramCredentialsRequest.parse(buffer, apiVersion);
            case VOTE:
                return VoteRequest.parse(buffer, apiVersion);
            case BEGIN_QUORUM_EPOCH:
                return BeginQuorumEpochRequest.parse(buffer, apiVersion);
            case END_QUORUM_EPOCH:
                return EndQuorumEpochRequest.parse(buffer, apiVersion);
            case DESCRIBE_QUORUM:
                return DescribeQuorumRequest.parse(buffer, apiVersion);
            case ALTER_PARTITION:
                return AlterPartitionRequest.parse(buffer, apiVersion);
            case UPDATE_FEATURES:
                return UpdateFeaturesRequest.parse(buffer, apiVersion);
            case ENVELOPE:
                return EnvelopeRequest.parse(buffer, apiVersion);
            case FETCH_SNAPSHOT:
                return FetchSnapshotRequest.parse(buffer, apiVersion);
            case DESCRIBE_CLUSTER:
                return DescribeClusterRequest.parse(buffer, apiVersion);
            case DESCRIBE_PRODUCERS:
                return DescribeProducersRequest.parse(buffer, apiVersion);
            case BROKER_REGISTRATION:
                return BrokerRegistrationRequest.parse(buffer, apiVersion);
            case BROKER_HEARTBEAT:
                return BrokerHeartbeatRequest.parse(buffer, apiVersion);
            case UNREGISTER_BROKER:
                return UnregisterBrokerRequest.parse(buffer, apiVersion);
            case DESCRIBE_TRANSACTIONS:
                return DescribeTransactionsRequest.parse(buffer, apiVersion);
            case LIST_TRANSACTIONS:
                return ListTransactionsRequest.parse(buffer, apiVersion);
            case ALLOCATE_PRODUCER_IDS:
                return AllocateProducerIdsRequest.parse(buffer, apiVersion);
            case CONSUMER_GROUP_HEARTBEAT:
                return ConsumerGroupHeartbeatRequest.parse(buffer, apiVersion);
            default:
                throw new AssertionError(String.format("ApiKey %s is not currently handled in `parseRequest`, the " +
                        "code should be updated to do so.", apiKey));
        }
    }
}
