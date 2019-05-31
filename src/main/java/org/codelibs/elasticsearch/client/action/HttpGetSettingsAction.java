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
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpGetSettingsAction extends HttpAction {

    protected final GetSettingsAction action;

    public HttpGetSettingsAction(final HttpClient client, final GetSettingsAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetSettingsRequest request, final ActionListener<GetSettingsResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final GetSettingsResponse getSettingsResponse = GetSettingsResponse.fromXContent(parser);
                listener.onResponse(getSettingsResponse);
            } catch (final Throwable t) {
                listener.onFailure(toElasticsearchException(response, t));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final GetSettingsRequest request) {
        // RestGetSettingsAction
        final CurlRequest curlRequest =
                client.getCurlRequest(GET, "/_settings/" + UrlUtils.joinAndEncode(",", request.names()), request.indices());
        curlRequest.param("human", Boolean.toString(request.humanReadable()));
        curlRequest.param("include_defaults", Boolean.toString(request.includeDefaults()));
        curlRequest.param("local", Boolean.toString(request.local()));
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }
}
