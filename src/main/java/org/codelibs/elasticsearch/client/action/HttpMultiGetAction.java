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
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpMultiGetAction extends HttpAction {

    protected final MultiGetAction action;

    public HttpMultiGetAction(final HttpClient client, final MultiGetAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final MultiGetRequest request, final ActionListener<MultiGetResponse> listener) {
        String source = null;
        try {
            final XContentBuilder builder = request.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS);
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to parse a request.", e);
        }
        client.getCurlRequest(GET, "/_mget").body(source).execute(response -> {
            try (final InputStream in = response.getContentAsStream()) {
                if (response.getHttpStatusCode() != 200) {
                    throw new ElasticsearchException("error: " + response.getHttpStatusCode());
                }
                final XContentParser parser = createParser(in);
                final MultiGetResponse multiGetResponse = MultiGetResponse.fromXContent(parser);
                listener.onResponse(multiGetResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }
}
