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
import java.util.ArrayList;
import java.util.List;

import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ingest.GetPipelineAction;
import org.elasticsearch.action.ingest.GetPipelineRequest;
import org.elasticsearch.action.ingest.GetPipelineResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.ingest.PipelineConfiguration;

public class HttpGetPipelineAction extends HttpAction {

    protected final GetPipelineAction action;

    public HttpGetPipelineAction(final HttpClient client, final GetPipelineAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetPipelineRequest request, final ActionListener<GetPipelineResponse> listener) {
        client.getCurlRequest(GET, "/_ingest/pipeline/" + String.join(",", request.getIds())).execute(response -> {
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final GetPipelineResponse getPipelineResponse = getGetPipelineResponse(parser);
                listener.onResponse(getPipelineResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, listener::onFailure);
    }

    protected GetPipelineResponse getGetPipelineResponse(final XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
        final List<PipelineConfiguration> pipelines = new ArrayList<>();
        while (parser.nextToken().equals(Token.FIELD_NAME)) {
            final String pipelineId = parser.currentName();
            parser.nextToken();
            final XContentBuilder contentBuilder = XContentBuilder.builder(parser.contentType().xContent());
            contentBuilder.generator().copyCurrentStructure(parser);
            final PipelineConfiguration pipeline =
                    new PipelineConfiguration(pipelineId, BytesReference.bytes(contentBuilder), contentBuilder.contentType());
            pipelines.add(pipeline);
        }
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.currentToken(), parser::getTokenLocation);
        return new GetPipelineResponse(pipelines);
    }
}
