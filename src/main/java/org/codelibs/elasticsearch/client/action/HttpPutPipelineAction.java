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

import java.io.IOException;
import java.io.InputStream;

import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ingest.PutPipelineAction;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.ingest.WritePipelineResponse;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpPutPipelineAction extends HttpAction {

    protected final PutPipelineAction action;

    public HttpPutPipelineAction(final HttpClient client, final PutPipelineAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final PutPipelineRequest request, final ActionListener<WritePipelineResponse> listener) {
        String source = null;
        try {
            source = XContentHelper.convertToJson(request.getSource(), true);
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to parse a reqsuest.", e);
        }
        client.getCurlRequest(PUT, "/_ingest/pipeline/" + request.getId())
                .param("timeout", (request.timeout() == null ? null : request.timeout().toString())).body(source).execute(response -> {
                    try (final InputStream in = response.getContentAsStream()) {
                        final XContentParser parser = createParser(in);
                        final WritePipelineResponse putPipelineResponse = getAcknowledgedResponse(parser, action::newResponse);
                        listener.onResponse(putPipelineResponse);
                    } catch (final Exception e) {
                        listener.onFailure(e);
                    }
                }, listener::onFailure);
    }
}
