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

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
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
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final FlushResponse flushResponse = FlushResponse.fromXContent(parser);
                listener.onResponse(flushResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final FlushRequest request) {
        // RestFlushAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_flush", request.indices());
        curlRequest.param("wait_if_ongoing", Boolean.toString(request.waitIfOngoing()));
        curlRequest.param("force", Boolean.toString(request.force()));
        return curlRequest;
    }
}
