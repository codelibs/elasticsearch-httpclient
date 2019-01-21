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

import java.io.IOException;
import java.util.Locale;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.codelibs.elasticsearch.client.util.UrlUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;

public class HttpIndexAction extends HttpAction {

    protected final IndexAction action;

    public HttpIndexAction(final HttpClient client, final IndexAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final IndexRequest request, final ActionListener<IndexResponse> listener) {
        String source = null;
        try {
            source = XContentHelper.convertToJson(request.source(), false, XContentType.JSON);
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to parse a request.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final IndexResponse indexResponse = IndexResponse.fromXContent(parser);
                listener.onResponse(indexResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    private CurlRequest getCurlRequest(final IndexRequest request) {
        // RestIndexAction
        final OpType opType = request.id() == null ? OpType.CREATE : request.opType();
        final boolean isPutMethod = request.id() != null && OpType.CREATE.equals(opType);
        final StringBuilder pathBuf = new StringBuilder(100).append('/').append(UrlUtils.encode(request.type())).append('/');
        if (request.id() != null) {
            pathBuf.append(UrlUtils.encode(request.id()));
        }
        final CurlRequest curlRequest = client.getCurlRequest(isPutMethod ? PUT : POST, pathBuf.toString(), request.index());
        if (request.routing() != null) {
            curlRequest.param("routing", request.routing());
        }
        if (request.parent() != null) {
            curlRequest.param("parent", request.parent());
        }
        if (request.getPipeline() != null) {
            curlRequest.param("pipeline", request.getPipeline());
        }
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        if (!RefreshPolicy.NONE.equals(request.getRefreshPolicy())) {
            curlRequest.param("refresh", request.getRefreshPolicy().getValue());
        }
        if (request.version() >= 0) {
            curlRequest.param("version", Long.toString(request.version()));
        }
        if (!VersionType.INTERNAL.equals(request.versionType())) {
            curlRequest.param("version_type", request.versionType().name().toLowerCase(Locale.ROOT));
        }
        if (!ActiveShardCount.DEFAULT.equals(request.waitForActiveShards())) {
            curlRequest.param("wait_for_active_shards", String.valueOf(getActiveShardsCountValue(request.waitForActiveShards())));
        }
        if (request.id() != null) {
            curlRequest.param("op_type", opType.getLowercase());
        }
        return curlRequest;
    }
}
