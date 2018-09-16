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
import org.codelibs.elasticsearch.client.util.UrlUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpGetFieldMappingsAction extends HttpAction {

    protected final GetFieldMappingsAction action;

    public HttpGetFieldMappingsAction(final HttpClient client, final GetFieldMappingsAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetFieldMappingsRequest request, final ActionListener<GetFieldMappingsResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final GetFieldMappingsResponse getFieldMappingsResponse = GetFieldMappingsResponse.fromXContent(parser);
                listener.onResponse(getFieldMappingsResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final GetFieldMappingsRequest request) {
        // RestGetFieldMappingsAction
        final StringBuilder pathSuffix = new StringBuilder(100).append("/_mapping/");
        if (request.types().length > 0) {
            pathSuffix.append(UrlUtils.joinAndEncode(",", request.types())).append('/');
        }
        pathSuffix.append("field/");
        if (request.fields().length > 0) {
            pathSuffix.append(UrlUtils.joinAndEncode(",", request.fields()));
        }
        final CurlRequest curlRequest = client.getCurlRequest(GET, pathSuffix.toString(), request.indices());
        curlRequest.param("include_defaults", Boolean.toString(request.includeDefaults()));
        curlRequest.param("local", Boolean.toString(request.local()));
        return curlRequest;
    }
}
