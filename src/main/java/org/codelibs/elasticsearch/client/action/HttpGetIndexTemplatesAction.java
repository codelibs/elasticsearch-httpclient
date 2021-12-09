/*
 * Copyright 2012-2021 CodeLibs Project and the Others.
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
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.xcontent.XContentParser;

public class HttpGetIndexTemplatesAction extends HttpAction {

    protected final GetIndexTemplatesAction action;

    public HttpGetIndexTemplatesAction(final HttpClient client, final GetIndexTemplatesAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetIndexTemplatesRequest request, final ActionListener<GetIndexTemplatesResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final GetIndexTemplatesResponse getIndexTemplatesResponse = GetIndexTemplatesResponse.fromXContent(parser);
                listener.onResponse(getIndexTemplatesResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final GetIndexTemplatesRequest request) {
        // RestGetIndexTemplateAction
        final CurlRequest curlRequest = client.getCurlRequest(GET, "/_template/" + UrlUtils.joinAndEncode(",", request.names()));
        curlRequest.param("local", Boolean.toString(request.local()));
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }
}
