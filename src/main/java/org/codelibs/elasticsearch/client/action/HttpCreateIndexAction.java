/*
 * Copyright 2012-2021 CodeLibs Project and the Others.
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
import java.util.Map;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;

public class HttpCreateIndexAction extends HttpAction {

    protected static final ParseField MAPPINGS = new ParseField("mappings");
    protected static final ParseField SETTINGS = new ParseField("settings");
    protected static final ParseField ALIASES = new ParseField("aliases");

    protected final CreateIndexAction action;

    public HttpCreateIndexAction(final HttpClient client, final CreateIndexAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final CreateIndexRequest request, final ActionListener<CreateIndexResponse> listener) {
        String source = null;
        try (final XContentBuilder builder = toXContent(request, JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)) {
            builder.flush();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to parse a request.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final CreateIndexResponse refreshResponse = CreateIndexResponse.fromXContent(parser);
                listener.onResponse(refreshResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected XContentBuilder toXContent(final CreateIndexRequest request, final XContentBuilder builder, final Params params)
            throws IOException {
        builder.startObject();
        innerToXContent(request, builder, params);
        builder.endObject();
        return builder;
    }

    protected XContentBuilder innerToXContent(final CreateIndexRequest request, final XContentBuilder builder, final Params params)
            throws IOException {
        builder.startObject(SETTINGS.getPreferredName());
        request.settings().toXContent(builder, params);
        builder.endObject();

        final String mappingSource = request.mappings().get("_doc");
        if (mappingSource != null) {
            try (final XContentParser createParser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE, mappingSource)) {
                final Map<String, Object> mappingMap = createParser.map();
                if (mappingMap.containsKey("_doc")) {
                    builder.field(MAPPINGS.getPreferredName(), mappingMap.get("_doc"));
                } else {
                    try (InputStream stream = new BytesArray(mappingSource).streamInput()) {
                        builder.rawField(MAPPINGS.getPreferredName(), stream, XContentType.JSON);
                    }
                }
            }
        } else {
            builder.startObject(MAPPINGS.getPreferredName());
            for (final Map.Entry<String, String> entry : request.mappings().entrySet()) {
                if ("properties".equals(entry.getKey()) || "dynamic_templates".equals(entry.getKey()) || "_source".equals(entry.getKey())) {
                    final Map<String, Object> sourceMap =
                            XContentHelper.convertToMap(new BytesArray(entry.getValue()), false, XContentType.JSON).v2();
                    builder.field(entry.getKey(), sourceMap.get(entry.getKey()));
                } else {
                    try (InputStream stream = new BytesArray(entry.getValue()).streamInput()) {
                        builder.rawField(entry.getKey(), stream, XContentType.JSON);
                    }
                }
            }
            builder.endObject();
        }

        builder.startObject(ALIASES.getPreferredName());
        for (final Alias alias : request.aliases()) {
            alias.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    protected CurlRequest getCurlRequest(final CreateIndexRequest request) {
        // RestCreateIndexAction
        final CurlRequest curlRequest = client.getCurlRequest(PUT, "/", request.index());
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        if (!ActiveShardCount.DEFAULT.equals(request.waitForActiveShards())) {
            curlRequest.param("wait_for_active_shards", String.valueOf(getActiveShardsCountValue(request.waitForActiveShards())));
        }
        return curlRequest;
    }
}
