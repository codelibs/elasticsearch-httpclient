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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheAction;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpClearIndicesCacheAction extends HttpAction {

    protected final ClearIndicesCacheAction action;

    public HttpClearIndicesCacheAction(final HttpClient client, final ClearIndicesCacheAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final ClearIndicesCacheRequest request, final ActionListener<ClearIndicesCacheResponse> listener) {
        String fields = null;
        if (request.fields() != null && request.fields().length > 0) {
            fields = String.join(",", request.fields());
        }
        client.getCurlRequest(POST, "/_cache/clear", request.indices()).param("fielddata", String.valueOf(request.fieldDataCache()))
                .param("query", String.valueOf(request.queryCache())).param("request", String.valueOf(request.requestCache()))
                .param("fields", fields).execute(response -> {
                    try (final InputStream in = response.getContentAsStream()) {
                        final XContentParser parser = createParser(in);
                        final ClearIndicesCacheResponse clearIndicesCacheResponse = ClearIndicesCacheResponse.fromXContent(parser);
                        listener.onResponse(clearIndicesCacheResponse);
                    } catch (final Exception e) {
                        listener.onFailure(toElasticsearchException(response, e));
                    }
                }, listener::onFailure);
    }
}
