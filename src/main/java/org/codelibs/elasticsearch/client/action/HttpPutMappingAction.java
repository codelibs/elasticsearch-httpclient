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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpPutMappingAction extends HttpAction {

    protected final PutMappingAction action;

    public HttpPutMappingAction(final HttpClient client, final PutMappingAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final PutMappingRequest request, final ActionListener<AcknowledgedResponse> listener) {
        getCurlRequest(request).body(request.source()).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final AcknowledgedResponse putMappingResponse = AcknowledgedResponse.fromXContent(parser);
                listener.onResponse(putMappingResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final PutMappingRequest request) {
        // RestPutMappingAction
        final String path = "/_mapping";
        final CurlRequest curlRequest = client.getCurlRequest(PUT, path, request.indices());
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }

}
