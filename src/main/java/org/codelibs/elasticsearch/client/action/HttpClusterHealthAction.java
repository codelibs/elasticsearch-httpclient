/*
 * Copyright 2012-2018 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.elasticsearch.client.action;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.codelibs.elasticsearch.client.io.stream.ByteArrayStreamOutput;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpClusterHealthAction extends HttpAction {

    protected final ClusterHealthAction action;

    public HttpClusterHealthAction(final HttpClient client, final ClusterHealthAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final ClusterHealthRequest request, final ActionListener<ClusterHealthResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final ClusterHealthResponse clusterHealthResponse = getClusterHealthResponse(parser);
                listener.onResponse(clusterHealthResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final ClusterHealthRequest request) {
        // RestClusterHealthAction
        final CurlRequest curlRequest =
                client.getCurlRequest(GET, "/_cluster/health"
                        + (request.indices() == null ? "" : "/" + String.join(",", request.indices())));
        curlRequest.param("wait_for_no_relocating_shards", Boolean.toString(request.waitForNoRelocatingShards()));
        curlRequest.param("wait_for_no_initializing_shards", Boolean.toString(request.waitForNoInitializingShards()));
        curlRequest.param("wait_for_nodes", request.waitForNodes());
        if (request.waitForStatus() != null) {
            try {
                curlRequest.param("wait_for_status", ClusterHealthStatus.fromValue(request.waitForStatus().value()).toString()
                        .toLowerCase());
            } catch (final IOException e) {
                throw new ElasticsearchException("Failed to parse a request.", e);
            }
        }
        if (request.waitForActiveShards() != null) {
            curlRequest.param("wait_for_active_shards", String.valueOf(getActiveShardsCountValue(request.waitForActiveShards())));
        }
        if (!ActiveShardCount.DEFAULT.equals(request.waitForActiveShards())) {
            curlRequest.param("wait_for_active_shards", request.waitForActiveShards().toString());
        }
        if (request.waitForEvents() != null) {
            curlRequest.param("wait_for_events", request.waitForEvents().toString());
        }
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }

    protected ClusterHealthResponse getClusterHealthResponse(final XContentParser parser) throws IOException {
        final ConstructingObjectParser<ClusterHealthResponse, Void> objectParser =
                new ConstructingObjectParser<>("cluster_health_response", true, parsedObjects -> {
                    try (final ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
                        int i = 0;

                        // ClusterStateHealth fields
                        final int numberOfNodes = (int) parsedObjects[i++];
                        final int numberOfDataNodes = (int) parsedObjects[i++];
                        final int activeShards = (int) parsedObjects[i++];
                        final int relocatingShards = (int) parsedObjects[i++];
                        final int activePrimaryShards = (int) parsedObjects[i++];
                        final int initializingShards = (int) parsedObjects[i++];
                        final int unassignedShards = (int) parsedObjects[i++];
                        final double activeShardsPercent = (double) parsedObjects[i++];
                        final String statusStr = (String) parsedObjects[i++];
                        final ClusterHealthStatus clusterHealthStatus = ClusterHealthStatus.fromString(statusStr);

                        @SuppressWarnings("unchecked")
                        // Can be absent if LEVEL == 'cluster'
                        // null because no level option in 6.3
                        final List<ClusterIndexHealth> indexList = null;
                        final int indices_size = (indexList != null && !indexList.isEmpty() ? indexList.size() : 0);

                        // ClusterHealthResponse fields
                        final String clusterName = (String) parsedObjects[i++];
                        final int numberOfPendingTasks = (int) parsedObjects[i++];
                        final int numberOfInFlightFetch = (int) parsedObjects[i++];
                        final int delayedUnassignedShards = (int) parsedObjects[i++];
                        final TimeValue taskMaxWaitingTime = new TimeValue((long) parsedObjects[i++]);
                        final boolean timedOut = (boolean) parsedObjects[i];

                        out.writeString(clusterName);
                        out.writeByte(clusterHealthStatus.value());

                        // write ClusterStateHealth to out
                        out.writeVInt(activePrimaryShards);
                        out.writeVInt(activeShards);
                        out.writeVInt(relocatingShards);
                        out.writeVInt(initializingShards);
                        out.writeVInt(unassignedShards);
                        out.writeVInt(numberOfNodes);
                        out.writeVInt(numberOfDataNodes);
                        out.writeByte(clusterHealthStatus.value());
                        out.writeVInt(indices_size);
                        for (int j = 0; j < indices_size; j++) {
                            indexList.get(i).writeTo(out);
                        }
                        out.writeDouble(activeShardsPercent);

                        out.writeInt(numberOfPendingTasks);
                        out.writeBoolean(timedOut);
                        out.writeInt(numberOfInFlightFetch);
                        out.writeInt(delayedUnassignedShards);
                        out.writeTimeValue(taskMaxWaitingTime);

                        return ClusterHealthResponse.readResponseFrom(out.toStreamInput());
                    } catch (final IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });

        // ClusterStateHealth fields
        objectParser.declareInt(ConstructingObjectParser.constructorArg(), NUMBER_OF_NODES_FIELD);
        objectParser.declareInt(ConstructingObjectParser.constructorArg(), NUMBER_OF_DATA_NODES_FIELD);
        objectParser.declareInt(ConstructingObjectParser.constructorArg(), ACTIVE_SHARDS_FIELD);
        objectParser.declareInt(ConstructingObjectParser.constructorArg(), RELOCATING_SHARDS_FIELD);
        objectParser.declareInt(ConstructingObjectParser.constructorArg(), ACTIVE_PRIMARY_SHARDS_FIELD);
        objectParser.declareInt(ConstructingObjectParser.constructorArg(), INITIALIZING_SHARDS_FIELD);
        objectParser.declareInt(ConstructingObjectParser.constructorArg(), UNASSIGNED_SHARDS_FIELD);
        objectParser.declareDouble(ConstructingObjectParser.constructorArg(), ACTIVE_SHARDS_PERCENT_AS_NUMBER_FIELD);
        objectParser.declareString(ConstructingObjectParser.constructorArg(), STATUS_FIELD);

        // ClusterHealthResponse fields
        objectParser.declareString(ConstructingObjectParser.constructorArg(), CLUSTER_NAME_FIELD);
        objectParser.declareInt(ConstructingObjectParser.constructorArg(), NUMBER_OF_PENDING_TASKS_FIELD);
        objectParser.declareInt(ConstructingObjectParser.constructorArg(), NUMBER_OF_IN_FLIGHT_FETCH_FIELD);
        objectParser.declareInt(ConstructingObjectParser.constructorArg(), DELAYED_UNASSIGNED_SHARDS_FIELD);
        objectParser.declareLong(ConstructingObjectParser.constructorArg(), TASK_MAX_WAIT_TIME_IN_QUEUE_IN_MILLIS_FIELD);
        objectParser.declareBoolean(ConstructingObjectParser.constructorArg(), TIMED_OUT_FIELD);

        return objectParser.apply(parser, null);
    }
}
