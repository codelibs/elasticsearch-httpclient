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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.codelibs.elasticsearch.client.io.stream.ByteArrayStreamOutput;
import org.codelibs.elasticsearch.client.util.UrlUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.cluster.metadata.DataStreamMetadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentParserUtils;

public class HttpGetAliasesAction extends HttpAction {

    protected final GetAliasesAction action;

    public HttpGetAliasesAction(final HttpClient client, final GetAliasesAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetAliasesRequest request, final ActionListener<GetAliasesResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final GetAliasesResponse getAliasesResponse = getGetAliasesResponse(parser);
                listener.onResponse(getAliasesResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final GetAliasesRequest request) {
        // RestGetAliasesAction
        final CurlRequest curlRequest =
                client.getCurlRequest(GET, "/_alias/" + UrlUtils.joinAndEncode(",", request.aliases()), request.indices());
        curlRequest.param("local", Boolean.toString(request.local()));
        return curlRequest;
    }

    protected GetAliasesResponse getGetAliasesResponse(final XContentParser parser) throws IOException {
        final ImmutableOpenMap.Builder<String, List<AliasMetadata>> aliasesMapBuilder = ImmutableOpenMap.builder();
        final Map<String, List<DataStreamAlias>> dataStreamAliases = new HashMap<>();

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        XContentParser.Token token;
        String index = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == Token.FIELD_NAME) {
                index = parser.currentName();
            } else if (token == Token.START_OBJECT) {
                while (parser.nextToken() == Token.FIELD_NAME) {
                    final String currentFieldName = parser.currentName();
                    if (ALIASES_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                        aliasesMapBuilder.put(index, getAliases(parser));
                        // TODO data_stream https://github.com/elastic/elasticsearch/commit/49da7ec4ecee51b2d713241e6d6a8bc98eef0681
                    } else {
                        parser.skipChildren();
                    }
                }
            }
        }

        final ImmutableOpenMap<String, List<AliasMetadata>> aliases = aliasesMapBuilder.build();

        try (final ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
            out.writeMap(aliases, StreamOutput::writeString, StreamOutput::writeList);
            if (out.getVersion().onOrAfter(DataStreamMetadata.DATA_STREAM_ALIAS_VERSION)) {
                out.writeMap(dataStreamAliases, StreamOutput::writeString, StreamOutput::writeList);
            }
            return action.getResponseReader().read(out.toStreamInput());
        }
    }

    public static List<AliasMetadata> getAliases(final XContentParser parser) throws IOException {
        final List<AliasMetadata> aliases = new ArrayList<>();
        Token token = parser.nextToken();
        if (token == null) {
            return aliases;
        }
        while ((token = parser.nextToken()) != Token.END_OBJECT) {
            if (token == Token.FIELD_NAME) {
                aliases.add(AliasMetadata.Builder.fromXContent(parser));
            }
        }
        return aliases;
    }
}
