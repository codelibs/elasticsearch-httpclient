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
import java.util.function.Supplier;

import org.codelibs.elasticsearch.client.HttpClient;
import org.codelibs.elasticsearch.client.io.stream.ByteArrayStreamOutput;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

public class HttpGetIndexAction extends HttpAction {

    protected final GetIndexAction action;

    public HttpGetIndexAction(final HttpClient client, final GetIndexAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetIndexRequest request, final ActionListener<GetIndexResponse> listener) {
        client.getCurlRequest(GET, "/", request.indices()).execute(response -> {
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final GetIndexResponse getIndexResponse = getGetIndexResponse(parser, action::newResponse);
                listener.onResponse(getIndexResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected GetIndexResponse getGetIndexResponse(final XContentParser parser, final Supplier<GetIndexResponse> newResponse)
            throws IOException {
        final List<String> indices = new ArrayList<>();
        final ImmutableOpenMap.Builder<String, List<AliasMetaData>> aliasesMapBuilder = ImmutableOpenMap.builder();
        final ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> mappingsMapBuilder = ImmutableOpenMap.builder();
        final ImmutableOpenMap.Builder<String, Settings> settingsMapBuilder = ImmutableOpenMap.builder();

        String index = null;
        XContentParser.Token token = parser.nextToken();
        while ((token = parser.nextToken()) != Token.END_OBJECT) {
            if (token == Token.FIELD_NAME) {
                index = parser.currentName();
                indices.add(index);
            } else if (token == Token.START_OBJECT) {
                while (parser.nextToken() == Token.FIELD_NAME) {
                    final String currentFieldName = parser.currentName();
                    if (ALIASES_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                        aliasesMapBuilder.put(index, getAliases(parser));
                    } else if (MAPPINGS_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                        mappingsMapBuilder.put(index, HttpGetMappingsAction.getMappings(parser));
                    } else if (SETTINGS_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                        settingsMapBuilder.put(index, getSettings(parser));
                    }
                }
            }
        }

        final ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = mappingsMapBuilder.build();
        final ImmutableOpenMap<String, List<AliasMetaData>> aliases = aliasesMapBuilder.build();
        final ImmutableOpenMap<String, Settings> settings = settingsMapBuilder.build();

        try (ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
            out.writeStringArray(indices.toArray(new String[indices.size()]));
            out.writeVInt(mappings.size());
            for (final ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> indexEntry : mappings) {
                out.writeString(indexEntry.key);
                out.writeVInt(indexEntry.value.size());
                for (final ObjectObjectCursor<String, MappingMetaData> mappingEntry : indexEntry.value) {
                    out.writeString(mappingEntry.key);
                    mappingEntry.value.writeTo(out);
                }
            }
            out.writeVInt(aliases.size());
            for (final ObjectObjectCursor<String, List<AliasMetaData>> indexEntry : aliases) {
                out.writeString(indexEntry.key);
                out.writeVInt(indexEntry.value.size());
                for (final AliasMetaData aliasEntry : indexEntry.value) {
                    aliasEntry.writeTo(out);
                }
            }
            out.writeVInt(settings.size());
            for (final ObjectObjectCursor<String, Settings> indexEntry : settings) {
                out.writeString(indexEntry.key);
                Settings.writeSettingsToStream(indexEntry.value, out);
            }
            final GetIndexResponse response = newResponse.get();
            response.readFrom(out.toStreamInput());
            return response;
        }
    }

    protected List<AliasMetaData> getAliases(final XContentParser parser) throws IOException {
        final List<AliasMetaData> aliases = new ArrayList<>();
        Token token = parser.nextToken();
        if (token == null) {
            return aliases;
        }
        while ((token = parser.nextToken()) != Token.END_OBJECT) {
            if (token == Token.FIELD_NAME) {
                aliases.add(AliasMetaData.Builder.fromXContent(parser));
            }
        }
        return aliases;
    }

    protected Settings getSettings(final XContentParser parser) throws IOException {
        if (parser.nextToken() == null) {
            return Settings.EMPTY;
        }
        return Settings.fromXContent(parser);
    }
}
