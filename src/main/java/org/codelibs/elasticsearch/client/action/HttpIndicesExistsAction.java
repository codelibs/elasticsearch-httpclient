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
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpIndicesExistsAction extends HttpAction {

    protected final IndicesExistsAction action;

    public HttpIndicesExistsAction(final HttpClient client, final IndicesExistsAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final IndicesExistsRequest request, final ActionListener<IndicesExistsResponse> listener) {
        client.getCurlRequest(HEAD, null, request.indices()).execute(response -> {
            boolean exists = false;
            switch (response.getHttpStatusCode()) {
            case 200:
                exists = true;
                break;
            case 404:
                exists = false;
                break;
            default:
                throw new ElasticsearchException("Unexpected status: " + response.getHttpStatusCode());
            }
            try {
                final IndicesExistsResponse indicesExistsResponse = new IndicesExistsResponse(exists);
                listener.onResponse(indicesExistsResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }
}
