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
import org.elasticsearch.action.ingest.GetPipelineAction;
import org.elasticsearch.action.ingest.GetPipelineRequest;
import org.elasticsearch.action.ingest.GetPipelineResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpGetPipelineAction extends HttpAction {

    protected final GetPipelineAction action;

    public HttpGetPipelineAction(final HttpClient client, final GetPipelineAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetPipelineRequest request, final ActionListener<GetPipelineResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final GetPipelineResponse getPipelineResponse = GetPipelineResponse.fromXContent(parser);
                listener.onResponse(getPipelineResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final GetPipelineRequest request) {
        // RestGetPipelineAction
        final CurlRequest curlRequest = client.getCurlRequest(GET, "/_ingest/pipeline/" + UrlUtils.joinAndEncode(",", request.getIds()));
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }
}
