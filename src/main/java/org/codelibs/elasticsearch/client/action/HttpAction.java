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
import java.io.InputStream;
import java.util.function.Function;

import org.codelibs.curl.Curl;
import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

public class HttpAction {

    protected static final ParseField SHARD_FIELD = new ParseField("shard");

    protected static final ParseField INDEX_FIELD = new ParseField("index");

    protected static final ParseField QUERY_FIELD = new ParseField("query");

    protected static final ParseField STATUS_FIELD = new ParseField("status");

    protected static final ParseField REASON_FIELD = new ParseField("reason");

    protected static final ParseField ACKNOWLEDGED_FIELD = new ParseField("acknowledged");

    protected static final ParseField ALIASES_FIELD = new ParseField("aliases");

    protected static final ParseField MAPPINGS_FIELD = new ParseField("mappings");

    protected static final ParseField FIELDS_FIELD = new ParseField("fields");

    protected static final ParseField SETTINGS_FIELD = new ParseField("settings");

    protected static final ParseField TYPE_FIELD = new ParseField("type");

    protected static final ParseField SEARCHABLE_FIELD = new ParseField("searchable");

    protected static final ParseField AGGREGATABLE_FIELD = new ParseField("aggregatable");

    protected static final ParseField INDICES_FIELD = new ParseField("indices");

    protected static final ParseField NON_SEARCHABLE_INDICES_FIELD = new ParseField("non_searchable_indices");

    protected static final ParseField NON_AGGREGATABLE_INDICES_FIELD = new ParseField("non_aggregatable_indices");

    protected static final ParseField _INDEX_FIELD = new ParseField("_index");

    protected static final ParseField _TYPE_FIELD = new ParseField("_type");

    protected static final ParseField _ID_FIELD = new ParseField("_id");

    protected static final ParseField _ROUTING_FIELD = new ParseField("_routing");

    protected static final ParseField _VERSION_FIELD = new ParseField("_version");

    protected static final ParseField EXPLANATION_FIELD = new ParseField("explanation");

    protected static final ParseField VALUE_FIELD = new ParseField("value");

    protected static final ParseField DESCRIPTION_FIELD = new ParseField("description");

    protected static final ParseField DETAILS_FIELD = new ParseField("details");

    protected static final ParseField CLUSTER_NAME_FIELD = new ParseField("cluster_name");

    protected static final ParseField TIMED_OUT_FIELD = new ParseField("timed_out");

    protected static final ParseField NUMBER_OF_NODES_FIELD = new ParseField("number_of_nodes");

    protected static final ParseField NUMBER_OF_DATA_NODES_FIELD = new ParseField("number_of_data_nodes");

    protected static final ParseField NUMBER_OF_PENDING_TASKS_FIELD = new ParseField("number_of_pending_tasks");

    protected static final ParseField NUMBER_OF_IN_FLIGHT_FETCH_FIELD = new ParseField("number_of_in_flight_fetch");

    protected static final ParseField DELAYED_UNASSIGNED_SHARDS_FIELD = new ParseField("delayed_unassigned_shards");

    protected static final ParseField TASK_MAX_WAIT_TIME_IN_QUEUE_FIELD = new ParseField("task_max_waiting_in_queue");

    protected static final ParseField TASK_MAX_WAIT_TIME_IN_QUEUE_IN_MILLIS_FIELD = new ParseField("task_max_waiting_in_queue_millis");

    protected static final ParseField ACTIVE_SHARDS_PERCENT_AS_NUMBER_FIELD = new ParseField("active_shards_percent_as_number");

    protected static final ParseField ACTIVE_SHARDS_PERCENT_FIELD = new ParseField("active_shards_percent");

    protected static final ParseField ACTIVE_PRIMARY_SHARDS_FIELD = new ParseField("active_primary_shards");

    protected static final ParseField ACTIVE_SHARDS_FIELD = new ParseField("active_shards");

    protected static final ParseField RELOCATING_SHARDS_FIELD = new ParseField("relocating_shards");

    protected static final ParseField INITIALIZING_SHARDS_FIELD = new ParseField("initializing_shards");

    protected static final ParseField UNASSIGNED_SHARDS_FIELD = new ParseField("unassigned_shards");

    protected static final ParseField EXPLANATIONS_FIELD = new ParseField("explanations");

    protected static final ParseField VALID_FIELD = new ParseField("valid");

    protected static final ParseField _SHARDS_FIELD = new ParseField("_shards");

    protected static final ParseField ERROR_FIELD = new ParseField("error");

    protected static final ParseField TASKS_FIELD = new ParseField("tasks");

    protected static final ParseField INSERT_ORDER_FIELD = new ParseField("insert_order");

    protected static final ParseField PRIORITY_FIELD = new ParseField("priority");

    protected static final ParseField SOURCE_FIELD = new ParseField("source");

    protected static final ParseField TIME_IN_QUEUE_MILLIS_FIELD = new ParseField("time_in_queue_millis");

    protected static final ParseField TIME_IN_QUEUE_FIELD = new ParseField("time_in_queue");

    protected static final ParseField EXECUTING_FIELD = new ParseField("executing");

    protected static final ParseField GET_FIELD = new ParseField("get");

    protected static final ParseField TOTAL_FIELD = new ParseField("total");

    protected static final ParseField SUCCESSFUL_FIELD = new ParseField("successful");

    protected static final ParseField FAILED_FIELD = new ParseField("failed");

    protected static final ParseField FAILURES_FIELD = new ParseField("failures");

    protected static final ParseField STATE_FIELD = new ParseField("state");

    protected static final ParseField PRIMARY_FIELD = new ParseField("primary");

    protected static final ParseField NODE_FIELD = new ParseField("node");

    protected static final ParseField RELOCATING_NODE_FIELD = new ParseField("relocating_node");

    protected static final ParseField EXPECTED_SHARD_SIZE_IN_BYTES_FIELD = new ParseField("expected_shard_size_in_bytes");

    protected static final ParseField ROUTING_FIELD = new ParseField("routing");

    protected static final ParseField FULL_NAME_FIELD = new ParseField("full_name");

    protected static final ParseField MAPPING_FIELD = new ParseField("mapping");

    protected static final ParseField UNASSIGNED_INFO_FIELD = new ParseField("unassigned_info");

    protected static final ParseField ALLOCATION_ID_FIELD = new ParseField("allocation_id");

    protected static final ParseField RECOVERY_SOURCE_FIELD = new ParseField("recovery_source");

    protected static final ParseField AT_FIELD = new ParseField("at");

    protected static final ParseField FAILED_ATTEMPTS_FIELD = new ParseField("failed_attempts");

    protected static final ParseField ALLOCATION_STATUS_FIELD = new ParseField("allocation_status");

    protected static final ParseField DELAYED_FIELD = new ParseField("delayed");


    protected static final Function<String, CurlRequest> GET = Curl::get;

    protected static final Function<String, CurlRequest> POST = Curl::post;

    protected static final Function<String, CurlRequest> PUT = Curl::put;

    protected static final Function<String, CurlRequest> DELETE = Curl::delete;

    protected static final Function<String, CurlRequest> HEAD = Curl::head;

    protected final HttpClient client;

    public HttpAction(final HttpClient client) {
        this.client = client;
    }

    protected XContentParser createParser(final InputStream in) throws IOException {
        final XContent xContent = XContentFactory.xContent(XContentType.JSON);
        return xContent.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, in);
    }

    protected <T extends BroadcastResponse> T getResponseFromXContent(final XContentParser parser, final Supplier<T> newResponse)
            throws IOException {
        ensureExpectedToken(Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
        parser.nextToken();
        ensureExpectedToken(Token.FIELD_NAME, parser.currentToken(), parser::getTokenLocation);
        String currentFieldName = parser.currentName(); // _SHARDS_FIELD
        int totalShards = 0;
        int successfulShards = 0;
        int failedShards = 0;
        final List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>();
        for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
            if (token == Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == Token.START_ARRAY) {
                if (FAILURES_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token != XContentParser.Token.START_OBJECT) {
                            throw new ElasticsearchException("failures array element should include an object");
                        }
                        shardFailures.add(getFailureFromXContent(parser));
                    }
                } else {
                    parser.skipChildren();
                }
            } else if (token.isValue()) {
                if (TOTAL_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    totalShards = parser.intValue();
                } else if (SUCCESSFUL_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    successfulShards = parser.intValue();
                } else if (FAILED_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    failedShards = parser.intValue();
                } else {
                    parser.skipChildren();
                }
            }
        }

        // BroadcastResponse
        try (ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
            out.writeVInt(totalShards);
            out.writeVInt(successfulShards);
            out.writeVInt(failedShards);
            out.writeVInt(shardFailures.size());
            for (final ShardOperationFailedException exp : shardFailures) {
                exp.writeTo(out);
            }
            final T response = newResponse.get();
            response.readFrom(out.toStreamInput());
            return response;
        }
    }

    protected List<AliasMetaData> getAliasesFromXContent(final XContentParser parser) throws IOException {
        final List<AliasMetaData> aliases = new ArrayList<>();
        Token token = parser.nextToken();
        if (token == null) {
            return aliases;
        }
        while ((token = parser.nextToken()) != Token.END_OBJECT) {
            if (token == Token.FIELD_NAME) {
                aliases.add(AliasMetaData.Builder.fromXContent(parser));
            }
        }
        return aliases;
    }

    protected ImmutableOpenMap<String, MappingMetaData> getMappings(final XContentParser parser) throws IOException {
        final ImmutableOpenMap.Builder<String, MappingMetaData> mappingsBuilder = ImmutableOpenMap.builder();
        String type = null;
        Token token = parser.nextToken();
        if (token == null) {
            return mappingsBuilder.build();
        }
        while ((token = parser.nextToken()) != Token.END_OBJECT) {
            if (token == Token.FIELD_NAME) {
                type = parser.currentName();
            } else if (token == Token.START_OBJECT) {
                final Map<String, Object> mapping = parser.mapOrdered();
                mappingsBuilder.put(type, new MappingMetaData(type, mapping));
            }
        }
        return mappingsBuilder.build();
    }

    protected Settings getSettings(final XContentParser parser) throws IOException {
        if (parser.nextToken() == null) {
            return Settings.EMPTY;
        }
        return Settings.fromXContent(parser);
    }

    protected <T extends AcknowledgedResponse> T getAcknowledgedResponse(final XContentParser parser, final Supplier<T> newResponse)
            throws IOException {
        boolean acknowledged = false;
        String currentFieldName = null;
        Token token = parser.nextToken();
        if (token != null) {
            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                if (token == Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == Token.VALUE_BOOLEAN) {
                    if (ACKNOWLEDGED_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                        acknowledged = parser.booleanValue();
                    }
                }
            }
        }

        try (ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
            out.writeBoolean(acknowledged);
            final T response = newResponse.get();
            response.readFrom(out.toStreamInput());
            return response;
        }
    }

    protected DefaultShardOperationFailedException getFailureFromXContent(final XContentParser parser) throws IOException {
        String index = null;
        ElasticsearchException reason = null;
        int shardId = 0;
        String currentFieldName = "";
        for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
            if (token == Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (SHARD_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    shardId = parser.intValue();
                } else if (INDEX_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    index = parser.text();
                } else if (REASON_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    reason = ElasticsearchException.fromXContent(parser);
                } else {
                    parser.skipChildren();
                }
            }
        }
        return new DefaultShardOperationFailedException(index, shardId, reason);
    }

}