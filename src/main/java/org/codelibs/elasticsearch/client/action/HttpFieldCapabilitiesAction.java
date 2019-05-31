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
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesAction;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpFieldCapabilitiesAction extends HttpAction {

    protected final FieldCapabilitiesAction action;

    public HttpFieldCapabilitiesAction(final HttpClient client, final FieldCapabilitiesAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final FieldCapabilitiesRequest request, final ActionListener<FieldCapabilitiesResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final FieldCapabilitiesResponse fieldCapabilitiesResponse = FieldCapabilitiesResponse.fromXContent(parser);
                listener.onResponse(fieldCapabilitiesResponse);
            } catch (final Throwable t) {
                listener.onFailure(toElasticsearchException(response, t));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final FieldCapabilitiesRequest request) {
        // RestFieldCapabilitiesAction
        final CurlRequest curlRequest = client.getCurlRequest(GET, "/_field_caps", request.indices());
        if (request.fields() != null) {
            curlRequest.param("fields", String.join(",", request.fields()));
        }
        return curlRequest;
    }
}
