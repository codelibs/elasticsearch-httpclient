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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

public class HttpAnalyzeAction extends HttpAction {

    protected final AnalyzeAction action;

    public HttpAnalyzeAction(final HttpClient client, final AnalyzeAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final AnalyzeAction.Request request, final ActionListener<AnalyzeAction.Response> listener) {
        String source = null;
        try (final XContentBuilder builder = toXContent(request, JsonXContent.contentBuilder())) {
            builder.flush();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to parse a request.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final AnalyzeAction.Response cancelTasksResponse = AnalyzeAction.Response.fromXContent(parser);
                listener.onResponse(cancelTasksResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final AnalyzeAction.Request request) {
        // RestAnalyzeAction
        final CurlRequest curlRequest =
                client.getCurlRequest(POST, "/_analyze", request.index() == null ? new String[0] : request.indices());
        return curlRequest;
    }

    protected XContentBuilder toXContent(final AnalyzeAction.Request request, final XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field("text", request.text());
        if (Strings.isNullOrEmpty(request.analyzer()) == false) {
            builder.field("analyzer", request.analyzer());
        }
        if (request.tokenizer() != null) {
            builder.field("tokenizer", request.tokenizer());
        }
        if (request.tokenFilters().size() > 0) {
            builder.field("filter", request.tokenFilters());
        }
        if (request.charFilters().size() > 0) {
            builder.field("char_filter", request.charFilters());
        }
        if (Strings.isNullOrEmpty(request.field()) == false) {
            builder.field("field", request.field());
        }
        if (request.explain()) {
            builder.field("explain", true);
        }
        if (request.attributes().length > 0) {
            builder.field("attributes", request.attributes());
        }
        if (Strings.isNullOrEmpty(request.normalizer()) == false) {
            builder.field("normalizer", request.normalizer());
        }
        return builder.endObject();
    }

}
