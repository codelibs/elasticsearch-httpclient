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
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpRefreshAction extends HttpAction {

    protected final RefreshAction action;

    public HttpRefreshAction(final HttpClient client, final RefreshAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final RefreshRequest request, final ActionListener<RefreshResponse> listener) {
        client.getCurlRequest(POST, "/_refresh", request.indices()).execute(response -> {
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final RefreshResponse refreshResponse = RefreshResponse.fromXContent(parser);
                listener.onResponse(refreshResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, listener::onFailure);
    }
}
