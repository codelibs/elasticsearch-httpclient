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
import org.elasticsearch.action.ingest.DeletePipelineAction;
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.WritePipelineResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpDeletePipelineAction extends HttpAction {

    protected final DeletePipelineAction action;

    public HttpDeletePipelineAction(final HttpClient client, final DeletePipelineAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final DeletePipelineRequest request, final ActionListener<WritePipelineResponse> listener) {
        client.getCurlRequest(DELETE, "/_ingest/pipeline/" + request.getId())
                .param("timeout", (request.timeout() == null ? null : request.timeout().toString())).execute(response -> {
                    try (final InputStream in = response.getContentAsStream()) {
                        final XContentParser parser = createParser(in);
                        final WritePipelineResponse deletePipelineResponse = getAcknowledgedResponse(parser, action::newResponse);
                        listener.onResponse(deletePipelineResponse);
                    } catch (final Exception e) {
                        listener.onFailure(toElasticsearchException(response, e));
                    }
                }, e -> unwrapElasticsearchException(listener, e));
    }
}
