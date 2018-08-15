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

import java.io.InputStream;

import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpClusterHealthAction extends HttpAction {

    protected final ClusterHealthAction action;

    public HttpClusterHealthAction(final HttpClient client, final ClusterHealthAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final ClusterHealthRequest request,
            final ActionListener<ClusterHealthResponse> listener) {
        String waitForStatus = null;
        try {
            if (request.waitForStatus() != null) {
                final ClusterHealthStatus clusterHealthStatus = ClusterHealthStatus.fromValue(request.waitForStatus().value());
                waitForStatus = clusterHealthStatus.toString().toLowerCase();
            }
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to parse a request.", e);
        }
        client.getCurlRequest(GET, "/_cluster/health" + (request.indices() == null ? "" : "/" + String.join(",", request.indices())))
                .param("wait_for_status", waitForStatus)
                .param("wait_for_no_relocating_shards", String.valueOf(request.waitForNoRelocatingShards()))
                .param("wait_for_no_initializing_shards", String.valueOf(request.waitForNoInitializingShards()))
                .param("wait_for_active_shards", (request.waitForActiveShards() == null ? null : request.waitForActiveShards().toString()))
                .param("wait_for_nodes", request.waitForNodes())
                .param("timeout", (request.timeout() == null ? null : request.timeout().toString())).execute(response -> {
            if (response.getHttpStatusCode() != 200) {
                throw new ElasticsearchException("error: " + response.getHttpStatusCode());
            }
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final ClusterHealthResponse clusterHealthResponse = getClusterHealthResponse(parser);
                listener.onResponse(clusterHealthResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
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
                    } catch (IOException e) {
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
