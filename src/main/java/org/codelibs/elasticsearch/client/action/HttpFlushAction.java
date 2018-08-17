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
import org.elasticsearch.action.admin.indices.flush.FlushAction;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpFlushAction extends HttpAction {

    protected final FlushAction action;

    public HttpFlushAction(final HttpClient client, final FlushAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final FlushRequest request, final ActionListener<FlushResponse> listener) {
        client.getCurlRequest(POST, "/_flush", request.indices()).param("wait_if_ongoing", String.valueOf(request.waitIfOngoing()))
                .param("force", String.valueOf(request.force())).execute(response -> {
                    try (final InputStream in = response.getContentAsStream()) {
                        final XContentParser parser = createParser(in);
                        final FlushResponse flushResponse = FlushResponse.fromXContent(parser);
                        listener.onResponse(flushResponse);
                    } catch (final Exception e) {
                        listener.onFailure(e);
                    }
                }, listener::onFailure);
    }
}