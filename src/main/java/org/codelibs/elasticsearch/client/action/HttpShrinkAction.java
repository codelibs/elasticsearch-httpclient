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

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.codelibs.elasticsearch.client.util.UrlUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeResponse;
import org.elasticsearch.action.admin.indices.shrink.ShrinkAction;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpShrinkAction extends HttpAction {

    protected final ShrinkAction action;

    public HttpShrinkAction(final HttpClient client, final ShrinkAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final ResizeRequest request, final ActionListener<ResizeResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final ResizeResponse resizeResponse = ResizeResponse.fromXContent(parser);
                listener.onResponse(resizeResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final ResizeRequest request) {
        // RestShrinkAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_shrink/" + UrlUtils.encode(request.getTargetIndexRequest().index()),
                request.getSourceIndex());
        return curlRequest;
    }
}
