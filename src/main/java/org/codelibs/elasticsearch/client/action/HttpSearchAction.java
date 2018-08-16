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
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpSearchAction extends HttpAction {

    protected final SearchAction action;

    public HttpSearchAction(final HttpClient client, final SearchAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final SearchRequest request, final ActionListener<SearchResponse> listener) {
        client.getCurlRequest(POST,
                (request.types() != null && request.types().length > 0 ? ("/" + String.join(",", request.types())) : "") + "/_search",
                request.indices())
                .param("scroll",
                        (request.scroll() != null && request.scroll().keepAlive() != null) ? request.scroll().keepAlive().toString() : null)
                .param("request_cache", request.requestCache() != null ? request.requestCache().toString() : null)
                .param("routing", request.routing()).param("preference", request.preference()).body(request.source().toString())
                .execute(response -> {
                    try (final InputStream in = response.getContentAsStream()) {
                        final XContentParser parser = createParser(in);
                        final SearchResponse searchResponse = SearchResponse.fromXContent(parser);
                        listener.onResponse(searchResponse);
                    } catch (final Exception e) {
                        listener.onFailure(e);
                    }
                }, listener::onFailure);
    }
}
