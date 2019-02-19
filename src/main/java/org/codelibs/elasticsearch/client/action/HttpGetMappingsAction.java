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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexNotFoundException;

public class HttpGetMappingsAction extends HttpAction {

    protected final GetMappingsAction action;

    public HttpGetMappingsAction(final HttpClient client, final GetMappingsAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetMappingsRequest request, final ActionListener<GetMappingsResponse> listener) {
        getCurlRequest(request).execute(response -> {
            if (response.getHttpStatusCode() == 404) {
                throw new IndexNotFoundException(String.join(",", request.indices()));
            }
            try (final XContentParser parser = createParser(response)) {
                final GetMappingsResponse getMappingsResponse = GetMappingsResponse.fromXContent(parser);
                listener.onResponse(getMappingsResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final GetMappingsRequest request) {
        // RestGetMappingAction
        final CurlRequest curlRequest = client.getCurlRequest(GET, "/_mapping", request.indices());
        curlRequest.param("local", Boolean.toString(request.local()));
        return curlRequest;
    }
}
