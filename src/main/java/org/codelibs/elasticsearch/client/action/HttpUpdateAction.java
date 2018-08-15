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
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpUpdateAction extends HttpAction {

    protected final UpdateAction action;

    public HttpUpdateAction(final HttpClient client, final UpdateAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final UpdateAction action, final UpdateRequest request, final ActionListener<UpdateResponse> listener) {
        String source = null;
        try {
            final XContentBuilder builder = request.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS);
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to parse a request.", e);
        }
        client.getCurlRequest(POST, "/" + request.type() + "/" + request.id() + "/_update", request.index()).param("routing", request.routing())
                .param("retry_on_conflict", String.valueOf(request.retryOnConflict())).param("version", String.valueOf(request.version()))
                .body(source).execute(response -> {
            try (final InputStream in = response.getContentAsStream()) {
                if (response.getHttpStatusCode() != 200 && response.getHttpStatusCode() != 201) {
                    throw new ElasticsearchException("error: " + response.getHttpStatusCode());
                }
                final XContentParser parser = createParser(in);
                final UpdateResponse updateResponse = UpdateResponse.fromXContent(parser);
                listener.onResponse(updateResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }
}
