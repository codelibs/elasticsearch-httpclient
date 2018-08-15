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
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.xcontent.XContentParser;

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
        client.getCurlRequest(PUT, "/" + request.type() + "/" + request.id(), request.index()).param("routing", request.routing()).body(source)
                .execute(response -> {
                    if (response.getHttpStatusCode() != 200 && response.getHttpStatusCode() != 201) {
                        throw new ElasticsearchException("error: " + response.getHttpStatusCode());
                    }
                    try (final InputStream in = response.getContentAsStream()) {
                        final XContentParser parser = createParser(in);
                        final IndexResponse indexResponse = IndexResponse.fromXContent(parser);
                        listener.onResponse(indexResponse);
                    } catch (final Exception e) {
                        listener.onFailure(e);
                    }
                }, listener::onFailure);
    }
}
