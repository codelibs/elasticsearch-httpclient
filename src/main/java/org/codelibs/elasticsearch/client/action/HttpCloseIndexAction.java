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

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpCloseIndexAction extends HttpAction {

    protected final CloseIndexAction action;

    public HttpCloseIndexAction(final HttpClient client, final CloseIndexAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final CloseIndexRequest request, final ActionListener<CloseIndexResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final CloseIndexResponse closeIndexResponse = fromXContent(parser);
                listener.onResponse(closeIndexResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    private static final ParseField SHARDS_ACKNOWLEDGED = new ParseField("shards_acknowledged");
    private static final ParseField ACKNOWLEDGED = new ParseField("acknowledged");
    private static final ConstructingObjectParser<CloseIndexResponse, Void> PARSER = new ConstructingObjectParser<>("close_index", true,
            args -> new CloseIndexResponse((boolean) args[0], (boolean) args[1]));

    static {
        PARSER.declareField(constructorArg(), (parser, context) -> parser.booleanValue(), ACKNOWLEDGED, ObjectParser.ValueType.BOOLEAN);
        PARSER.declareField(constructorArg(), (parser, context) -> parser.booleanValue(), SHARDS_ACKNOWLEDGED,
                ObjectParser.ValueType.BOOLEAN);
    }

    private CloseIndexResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    protected CurlRequest getCurlRequest(final CloseIndexRequest request) {
        // RestCloseIndexAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_close", request.indices());
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }
}
