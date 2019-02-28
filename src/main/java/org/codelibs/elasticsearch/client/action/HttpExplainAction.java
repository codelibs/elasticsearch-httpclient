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

import java.io.IOException;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.codelibs.elasticsearch.client.util.UrlUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.explain.ExplainAction;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpExplainAction extends HttpAction {

    protected final ExplainAction action;

    public HttpExplainAction(final HttpClient client, final ExplainAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final ExplainRequest request, final ActionListener<ExplainResponse> listener) {
        String source = null;
        try (final XContentBuilder builder =
                XContentFactory.jsonBuilder().startObject().field(QUERY_FIELD.getPreferredName(), request.query()).endObject()) {
            builder.flush();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to parse a request.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final ExplainResponse explainResponse = ExplainResponse.fromXContent(parser, true);
                listener.onResponse(explainResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final ExplainRequest request) {
        // RestExplainAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_explain/" + UrlUtils.encode(request.id()), request.index());
        if (request.routing() != null) {
            curlRequest.param("routing", request.routing());
        }
        if (request.preference() != null) {
            curlRequest.param("preference", request.preference());
        }
        if (request.query() != null) {
            //
        }
        if (request.storedFields() != null) {
            curlRequest.param("stored_fields", String.join(",", request.storedFields()));
        }
        return curlRequest;
    }

}
