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

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpSearchAction extends HttpAction {

    protected final SearchAction action;

    public HttpSearchAction(final HttpClient client, final SearchAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final SearchRequest request, final ActionListener<SearchResponse> listener) {
        getCurlRequest(request).body(request.source() != null ? request.source().toString() : null).execute(response -> {
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final SearchResponse searchResponse = SearchResponse.fromXContent(parser);
                listener.onResponse(searchResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final SearchRequest request) {
        // RestSearchAction
        CurlRequest curlRequest = client.getCurlRequest(POST, "/_search", request.indices());
        curlRequest.param("batched_reduce_size", Integer.toString(request.getBatchedReduceSize()));
        curlRequest.param("pre_filter_shard_size", Integer.toString(request.getPreFilterShardSize()));
        if (request.getMaxConcurrentShardRequests() > 0) {
            curlRequest.param("max_concurrent_shard_requests", Integer.toString(request.getMaxConcurrentShardRequests()));
        }
        if (request.allowPartialSearchResults() != null) {
            curlRequest.param("allow_partial_search_results", request.allowPartialSearchResults().toString());
        }
        if (!SearchType.DEFAULT.equals(request.searchType())) {
            curlRequest.param("search_type", request.searchType().name().toLowerCase());
        }
        if (request.requestCache() != null) {
            curlRequest.param("request_cache", request.requestCache().toString());
        }
        if (request.scroll() != null) {
            curlRequest.param("scroll", request.scroll().keepAlive().toString());
        }
        if (request.routing() != null) {
            curlRequest.param("routing", request.routing());
        }
        if (request.preference() != null) {
            curlRequest.param("preference", request.preference());
        }
        return curlRequest;
    }
}
