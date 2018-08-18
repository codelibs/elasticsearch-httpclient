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
import java.util.List;
import java.util.logging.Logger;

import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;

public class HttpBulkAction extends HttpAction {

    protected static final Logger logger = Logger.getLogger(HttpBulkAction.class.getName());

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
                    final XContentBuilder builder =
                            ((CreateIndexRequest) req).toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS);
                    buf.append(BytesReference.bytes(builder).utf8ToString());
                    buf.append('\n');
                    break;
                }
                case 2: { // UPDATE
                    final XContentBuilder builder =
                            ((UpdateRequest) req).toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS);
                    buf.append(BytesReference.bytes(builder).utf8ToString());
                    buf.append('\n');
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
        logger.fine(() -> "bulk request:\n" + buf);
        client.getCurlRequest(POST, "/_bulk").body(buf.toString()).execute(response -> {
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final BulkResponse bulkResponse = BulkResponse.fromXContent(parser);
                listener.onResponse(bulkResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }

    protected String getStringfromDocWriteRequest(final DocWriteRequest<?> request) {
        final StringBuilder buf = new StringBuilder(100);
        buf.append("{\"").append(request.opType().getLowercase()).append("\":{");
        appendStr(buf, _INDEX_FIELD.getPreferredName(), request.index());
        if (request.type() != null) {
            appendStr(buf.append(','), _TYPE_FIELD.getPreferredName(), request.type());
        }
        if (request.id() != null) {
            appendStr(buf.append(','), _ID_FIELD.getPreferredName(), request.id());
        }
        if (request.routing() != null) {
            appendStr(buf.append(','), _ROUTING_FIELD.getPreferredName(), request.routing());
        }
        buf.append(',').append('"').append(_VERSION_FIELD.getPreferredName()).append("\":").append(request.version());
        buf.append('}');
        buf.append('}');
        return buf.toString();
    }

    protected StringBuilder appendStr(final StringBuilder buf, final String key, final String value) {
        return buf.append('"').append(key).append("\":\"").append(value).append('"');
    }
}
