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

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpIndicesAliasesAction extends HttpAction {

    protected final IndicesAliasesAction action;

    public HttpIndicesAliasesAction(final HttpClient client, final IndicesAliasesAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final IndicesAliasesRequest request, final ActionListener<IndicesAliasesResponse> listener) {
        String source = null;
        try {
            final XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startArray("actions");
            for (final AliasActions aliasAction : request.getAliasActions()) {
                builder.startObject().startObject(aliasAction.actionType().toString().toLowerCase());
                builder.array("indices", aliasAction.indices());
                builder.array("aliases", aliasAction.aliases());
                if (aliasAction.filter() != null) {
                    builder.field("filter", aliasAction.filter());
                }
                if (aliasAction.indexRouting() != null) {
                    builder.field("index_routing", aliasAction.indexRouting());
                }
                if (aliasAction.searchRouting() != null) {
                    builder.field("search_routing", aliasAction.searchRouting());
                }
                builder.endObject().endObject();
            }
            builder.endArray().endObject();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to parse a request.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final IndicesAliasesResponse indicesAliasesResponse = IndicesAliasesResponse.fromXContent(parser);
                listener.onResponse(indicesAliasesResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final IndicesAliasesRequest request) {
        // RestIndicesAliasesAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_aliases");
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }
}
