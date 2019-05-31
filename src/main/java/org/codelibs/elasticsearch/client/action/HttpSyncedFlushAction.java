/*
 * Copyright 2012-2019 CodeLibs Project and the Others.
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

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.codelibs.elasticsearch.client.io.stream.ByteArrayStreamOutput;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushAction;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushRequest;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushResponse;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.flush.ShardsSyncedFlushResult;
import org.elasticsearch.indices.flush.SyncedFlushService;

public class HttpSyncedFlushAction extends HttpAction {

    protected final SyncedFlushAction action;

    public HttpSyncedFlushAction(final HttpClient client, final SyncedFlushAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final SyncedFlushRequest request, final ActionListener<SyncedFlushResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final SyncedFlushResponse syncedFlushResponse = getSyncedFlushResponse(parser, action::newResponse);
                listener.onResponse(syncedFlushResponse);
            } catch (final Throwable t) {
                listener.onFailure(toElasticsearchException(response, t));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final SyncedFlushRequest request) {
        // RestSyncedFlushAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_flush/synced", request.indices());
        return curlRequest;
    }

    protected SyncedFlushResponse getSyncedFlushResponse(final XContentParser parser, final Supplier<SyncedFlushResponse> newResponse)
            throws IOException {
        ensureExpectedToken(Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);

        //  Fields for ShardCounts
        ShardCounts totalCounts = null;
        final Map<String, List<ShardsSyncedFlushResult>> shardsResultPerIndex = new HashMap<>();
        final XContentLocation startLoc = parser.getTokenLocation();
        for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
            if (_SHARDS_FIELD.match(parser.currentName(), LoggingDeprecationHandler.INSTANCE)) {
                ensureExpectedToken(Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                totalCounts = ShardCounts.fromXContent(parser);
            } else if (token == Token.START_OBJECT) {
                final String index = parser.currentName();
                final String uuid = ""; // cannot know from the info returned at REST
                final Index idx = new Index(index, uuid);
                shardsResultPerIndex.put(index, parseShardsSyncedFlushResults(parser, idx));
            }
        }

        try (final ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {

            if (totalCounts == null) {
                throw new ParsingException(startLoc, "Unable to reconstruct object. Total counts for shards couldn't be parsed.");
            }

            out.writeInt(totalCounts.total);
            out.writeInt(totalCounts.successful);
            out.writeInt(totalCounts.failed);
            out.writeInt(shardsResultPerIndex.size());
            for (final Map.Entry<String, List<ShardsSyncedFlushResult>> entry : shardsResultPerIndex.entrySet()) {
                out.writeString(entry.getKey());
                out.writeInt(entry.getValue().size());
                for (final ShardsSyncedFlushResult shardsSyncedFlushResult : entry.getValue()) {
                    shardsSyncedFlushResult.writeTo(out);
                }
            }
            final SyncedFlushResponse response = newResponse.get();
            response.readFrom(out.toStreamInput());
            return response;
        }
    }

    protected List<ShardsSyncedFlushResult> parseShardsSyncedFlushResults(final XContentParser parser, final Index index)
            throws IOException {
        // "failures" fields
        final List<ShardsSyncedFlushResult> shardsSyncedFlushResults = new ArrayList<>();
        int total = 0;
        for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
            if (token == Token.START_ARRAY) {
                final String currentFieldName = parser.currentName();
                if (FAILURES_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    parseFailuresField(parser, index, shardsSyncedFlushResults, total);
                } else {
                    parser.skipChildren();
                }
            } else if (token.isValue()) {
                final String currentFieldName = parser.currentName();
                if (TOTAL_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    total = parser.intValue();
                } else if (SUCCESSFUL_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    parser.intValue();
                } else if (FAILED_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    parser.intValue();
                } else {
                    parser.skipChildren();
                }
            }
        }

        return shardsSyncedFlushResults;
    }

    protected void parseFailuresField(final XContentParser parser, final Index index,
            final List<ShardsSyncedFlushResult> shardsSyncedFlushResults, final int total) throws IOException {
        Token token;
        while ((token = parser.nextToken()) != Token.END_ARRAY) {
            ensureExpectedToken(Token.START_OBJECT, token, parser::getTokenLocation);
            shardsSyncedFlushResults.add(parseShardFailuresResults(parser, index, total));
        }
    }

    protected ShardsSyncedFlushResult parseShardFailuresResults(final XContentParser parser, final Index index, final int totalShards)
            throws IOException {
        ensureExpectedToken(Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);

        String failureReason = null;
        int shardIdValue = 0;
        final Map<ShardRouting, SyncedFlushService.ShardSyncedFlushResponse> shardResponses = new HashMap<>();
        for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
            if (token == Token.START_OBJECT) {
                if (ROUTING_FIELD.match(parser.currentName(), LoggingDeprecationHandler.INSTANCE)) {
                    shardResponses.put(parseShardRouting(parser), new SyncedFlushService.ShardSyncedFlushResponse(failureReason));
                } else {
                    parser.skipChildren();
                }
            } else if (token.isValue()) {
                final String currentFieldName = parser.currentName();
                if (SHARD_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    shardIdValue = parser.intValue();
                } else if (REASON_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    failureReason = parser.text();
                } else {
                    parser.skipChildren();
                }
            }
        }

        if (shardResponses.isEmpty()) {
            return new ShardsSyncedFlushResult(new ShardId(index, shardIdValue), totalShards, failureReason);
        } else {
            final String syncId = ""; // cannot know from the info returned at REST
            return new ShardsSyncedFlushResult(new ShardId(index, shardIdValue), syncId, totalShards, shardResponses);
        }
    }

    protected ShardRouting parseShardRouting(final XContentParser parser) throws IOException {
        final ConstructingObjectParser<ShardRouting, Void> objectParser = new ConstructingObjectParser<>("routing", true, a -> {
            try (final ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
                int i = 0;

                final ShardRoutingState state = ShardRoutingState.valueOf((String) a[i++]);
                final boolean primary = (boolean) a[i++];
                final String currentNodeId = (String) a[i++];
                final String relocatingNodeId = (String) a[i++];
                final int shardIdValue = (int) a[i++];
                final String index = (String) a[i++];
                final long expectedShardSize = (long) a[i++];
                final String uuid = ""; // cannot know from the info returned at REST
                final ShardId shardId = new ShardId(new Index(index, uuid), shardIdValue);
                final UnassignedInfo unassignedInfo = (UnassignedInfo) a[i++];
                final AllocationId allocationId = (AllocationId) a[i++];
                final RecoverySource recoverySource = (RecoverySource) a[i++];

                out.writeOptionalString(currentNodeId);
                out.writeOptionalString(relocatingNodeId);
                out.writeBoolean(primary);
                out.writeByte(state.value());
                if (state == ShardRoutingState.UNASSIGNED || state == ShardRoutingState.INITIALIZING) {
                    recoverySource.writeTo(out);
                }
                out.writeOptionalWriteable(unassignedInfo);
                out.writeOptionalWriteable(allocationId);
                if (state == ShardRoutingState.RELOCATING || state == ShardRoutingState.INITIALIZING) {
                    out.writeLong(expectedShardSize);
                }

                return new ShardRouting(shardId, out.toStreamInput());
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        objectParser.declareString(constructorArg(), STATE_FIELD);
        objectParser.declareBoolean(constructorArg(), PRIMARY_FIELD);
        objectParser.declareString(constructorArg(), NODE_FIELD);
        objectParser.declareString(constructorArg(), RELOCATING_NODE_FIELD);
        objectParser.declareInt(constructorArg(), SHARD_FIELD);
        objectParser.declareString(constructorArg(), INDEX_FIELD);
        objectParser.declareLong(constructorArg(), EXPECTED_SHARD_SIZE_IN_BYTES_FIELD);
        objectParser.declareObject(optionalConstructorArg(), (p, c) -> {
            try {
                return getUnassignedInfo(p);
            } catch (final Exception e) {
                throw new ElasticsearchException("Failed to create SyncedFlushResponse.", e);
            }
        }, UNASSIGNED_INFO_FIELD);
        objectParser.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> AllocationId.fromXContent(p),
                ALLOCATION_ID_FIELD);
        objectParser
                .declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> getRecoverySource(p), RECOVERY_SOURCE_FIELD);

        return objectParser.apply(parser, null);
    }

    protected UnassignedInfo getUnassignedInfo(final XContentParser parser) throws Exception {
        ensureExpectedToken(Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);

        UnassignedInfo.Reason reason = null;
        long unassignedTimeMillis = 0;
        int failedAllocations = 0;
        boolean delayed = false;
        UnassignedInfo.AllocationStatus allocationStatus = null;
        for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
            if (token.isValue()) {
                final String currentFieldName = parser.currentName();
                if (REASON_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    reason = UnassignedInfo.Reason.values()[parser.intValue()];
                } else if (AT_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    final SimpleDateFormat formatter = new SimpleDateFormat(Joda.forPattern("dateOptionalTime").pattern());
                    unassignedTimeMillis = formatter.parse(parser.text()).getTime();
                } else if (FAILED_ATTEMPTS_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    failedAllocations = parser.intValue();
                } else if (DELAYED_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    delayed = parser.booleanValue();
                } else if (DETAILS_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    parser.text();
                } else if (ALLOCATION_STATUS_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    allocationStatus = UnassignedInfo.AllocationStatus.values()[parser.intValue()];
                } else {
                    parser.skipChildren();
                }
            }
        }

        return new UnassignedInfo(reason, null, null, failedAllocations, unassignedTimeMillis * 1000000L, unassignedTimeMillis, delayed,
                allocationStatus);
    }

    protected RecoverySource getRecoverySource(final XContentParser parser) throws IOException {
        ensureExpectedToken(Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);

        byte type = -1;
        for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
            if (token.isValue()) {
                if (TYPE_FIELD.match(parser.currentName(), LoggingDeprecationHandler.INSTANCE)) {
                    type = (byte) parser.intValue();
                } else {
                    parser.skipChildren();
                }
            }
        }

        try (final ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
            out.writeByte(type);
            return RecoverySource.readFrom(out.toStreamInput());
        }
    }

    private static final class ShardCounts {

        private static final ConstructingObjectParser<ShardCounts, Void> PARSER = new ConstructingObjectParser<>("shardcounts",
                a -> new ShardCounts((Integer) a[0], (Integer) a[1], (Integer) a[2]));
        static {
            PARSER.declareInt(constructorArg(), TOTAL_FIELD);
            PARSER.declareInt(constructorArg(), SUCCESSFUL_FIELD);
            PARSER.declareInt(constructorArg(), FAILED_FIELD);
        }

        private final int total;
        private final int successful;
        private final int failed;

        ShardCounts(final int total, final int successful, final int failed) {
            this.total = total;
            this.successful = successful;
            this.failed = failed;
        }

        public static ShardCounts fromXContent(final XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }
}
