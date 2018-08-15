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
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpSearchAction extends HttpAction {

    protected final SearchAction action;

    public HttpSearchAction(final HttpClient client, final SearchAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final BulkAction action, final BulkRequest request, final ActionListener<BulkResponse> listener) {
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
        client.getCurlRequest(POST, "/_bulk").body(buf.toString()).execute(response -> {
            if (response.getHttpStatusCode() != 200) {
                throw new ElasticsearchException("error: " + response.getHttpStatusCode());
            }
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
        StringBuilder sb = new StringBuilder(100).append("{\"");
        sb.append(request.opType().getLowercase()).append("\":{\"").append(_INDEX_FIELD).append("\":\"").append(request.index())
                .append("\",\"").append(_TYPE_FIELD).append("\":\"").append(request.type()).append("\",\"").append(_ID_FIELD)
                .append("\":\"").append(request.id()).append("\",\"").append(_ROUTING_FIELD).append("\":\"").append(request.routing())
                .append("\",\"").append(_VERSION_FIELD).append("\":\"").append(request.version()).append("\"}}");
        return sb.toString();
    }
}
