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

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class HttpSearchAction extends HttpAction {

    protected final SearchAction action;

    public HttpSearchAction(final HttpClient client, final SearchAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final SearchRequest request, final ActionListener<SearchResponse> listener) {
        getCurlRequest(request).body(getQuerySource(request)).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final SearchResponse searchResponse = SearchResponse.fromXContent(parser);
                if (searchResponse.getHits() == null) {
                    listener.onFailure(toElasticsearchException(response, new ElasticsearchException("hits is null.")));
                } else {
                    listener.onResponse(searchResponse);
                }
            } catch (final Throwable t) {
                listener.onFailure(toElasticsearchException(response, t));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected String getQuerySource(final SearchRequest request) {
        final SearchSourceBuilder source = request.source();
        if (source != null) {
            try {
                return XContentHelper.toXContent(source, XContentType.JSON, ToXContent.EMPTY_PARAMS, false).utf8ToString();
            } catch (final IOException e) {
                throw new ElasticsearchException(e);
            }
        }
        return null;
    }

    protected CurlRequest getCurlRequest(final SearchRequest request) {
        // RestSearchAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_search", request.indices());
        curlRequest.param("typed_keys", "true");
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
        curlRequest.param("ccs_minimize_roundtrips", Boolean.toString(request.isCcsMinimizeRoundtrips()));
        return curlRequest;
    }
}
