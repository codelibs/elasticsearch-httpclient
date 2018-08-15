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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class GetAliasesAction extends HttpAction {

    protected final GetAliasesAction action;

    public HttpGetAliasesAction(final HttpClient client, final GetAliasesAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetAliasesRequest request,
            final ActionListener<GetAliasesResponse> listener) {
         client.getCurlRequest(GET, "/_alias/" + String.join(",", request.aliases()), request.indices()).execute(response -> {
            if (response.getHttpStatusCode() != 200) {
                throw new ElasticsearchException("error: " + response.getHttpStatusCode());
            }
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final GetAliasesResponse getAliasesResponse = getGetAliasesResponse(parser, action::newResponse);
                listener.onResponse(getAliasesResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }

    protected GetAliasesResponse getGetAliasesResponse(final XContentParser parser, final Supplier<GetAliasesResponse> newResponse)
            throws IOException {
        @SuppressWarnings("unchecked")
        final ImmutableOpenMap.Builder<String, List<AliasMetaData>> aliasesMapBuilder = ImmutableOpenMap.builder();

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
        XContentParser.Token token;
        String index = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == Token.FIELD_NAME) {
                index = parser.currentName();
            } else if (token == Token.START_OBJECT) {
                while (parser.nextToken() == Token.FIELD_NAME) {
                    final String currentFieldName = parser.currentName();
                    if (ALIASES_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                        aliasesMapBuilder.put(index, getAliasesFromXContent(parser));
                    } else {
                        parser.skipChildren();
                    }
                }
            }
        }

        final ImmutableOpenMap<String, List<AliasMetaData>> aliases = aliasesMapBuilder.build();

        try (final ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
            out.writeVInt(aliases.size());
            for (ObjectObjectCursor<String, List<AliasMetaData>> entry : aliases) {
                out.writeString(entry.key);
                out.writeVInt(entry.value.size());
                for (AliasMetaData aliasMetaData : entry.value) {
                    aliasMetaData.writeTo(out);
                }
            }
            final GetAliasesResponse response = newResponse.get();
            response.readFrom(out.toStreamInput());
            return response;
        }
    }
}
