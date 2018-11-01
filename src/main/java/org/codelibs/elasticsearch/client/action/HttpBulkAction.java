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
import java.util.List;
import java.util.Locale;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;

public class HttpBulkAction extends HttpAction {

    protected final BulkAction action;

    public HttpBulkAction(final HttpClient client, final BulkAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final BulkRequest request, final ActionListener<BulkResponse> listener) {
        // http://ndjson.org/
        final StringBuilder buf = new StringBuilder(10000);
        try {
            @SuppressWarnings("rawtypes")
            final List<DocWriteRequest> bulkRequests = request.requests();
            for (@SuppressWarnings("rawtypes")
            final DocWriteRequest req : bulkRequests) {
                buf.append(getStringfromDocWriteRequest(req));
                buf.append('\n');
                switch (req.opType().getId()) {
                case 0: { // INDEX
                    buf.append(XContentHelper.convertToJson(((IndexRequest) req).source(), false, XContentType.JSON));
                    buf.append('\n');
                    break;
                }
                case 1: { // CREATE
                    buf.append(XContentHelper.convertToJson(((IndexRequest) req).source(), false, XContentType.JSON));
                    buf.append('\n');
                    break;
                }
                case 2: { // UPDATE
                    try (final XContentBuilder builder =
                            ((UpdateRequest) req).toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)) {
                        builder.flush();
                        buf.append(BytesReference.bytes(builder).utf8ToString());
                        buf.append('\n');
                    }
                    break;
                }
                case 3: { // DELETE
                    break;
                }
                default:
                    break;
                }
            }
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to parse a request.", e);
        }
        getCurlRequest(request).body(buf.toString()).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final BulkResponse bulkResponse = BulkResponse.fromXContent(parser);
                listener.onResponse(bulkResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final BulkRequest request) {
        // RestBulkAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_bulk");
        if (!ActiveShardCount.DEFAULT.equals(request.waitForActiveShards())) {
            curlRequest.param("wait_for_active_shards", String.valueOf(getActiveShardsCountValue(request.waitForActiveShards())));
        }
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        if (!RefreshPolicy.NONE.equals(request.getRefreshPolicy())) {
            curlRequest.param("refresh", request.getRefreshPolicy().getValue());
        }
        return curlRequest;
    }

    protected String getStringfromDocWriteRequest(final DocWriteRequest<?> request) {
        final StringBuilder buf = new StringBuilder(100);
        buf.append("{\"").append(request.opType().getLowercase()).append("\":{");
        appendStr(buf, "_index", request.index());
        if (request.type() != null) {
            appendStr(buf.append(','), "_type", request.type());
        }
        if (request.id() != null) {
            appendStr(buf.append(','), "_id", request.id());
        }
        if (request.routing() != null) {
            appendStr(buf.append(','), "routing", request.routing());
        }
        if (request.parent() != null) {
            appendStr(buf.append(','), "parent", request.parent());
        }
        if (request.version() >= 0) {
            buf.append(',').append('"').append("version").append("\":").append(request.version());
        }
        if (request.versionType() != null) {
            appendStr(buf.append(','), "version_type", request.versionType().name().toLowerCase(Locale.ROOT));
        }
        buf.append('}');
        buf.append('}');
        return buf.toString();
    }

    protected StringBuilder appendStr(final StringBuilder buf, final String key, final String value) {
        return buf.append('"').append(key).append("\":\"").append(value).append('"');
    }
}
