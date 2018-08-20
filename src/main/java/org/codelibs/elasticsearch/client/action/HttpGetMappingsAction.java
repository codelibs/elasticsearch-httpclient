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
import java.util.Map;
import java.util.function.Supplier;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.codelibs.elasticsearch.client.io.stream.ByteArrayStreamOutput;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.IndexNotFoundException;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

public class HttpGetMappingsAction extends HttpAction {

    protected final GetMappingsAction action;

    public HttpGetMappingsAction(final HttpClient client, final GetMappingsAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetMappingsRequest request, final ActionListener<GetMappingsResponse> listener) {
        getCurlRequest(request).execute(response -> {
            if (response.getHttpStatusCode() == 404) {
                throw new IndexNotFoundException(String.join(",", request.indices()));
            }
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final GetMappingsResponse getMappingsResponse = getGetMappingsResponse(parser, action::newResponse);
                listener.onResponse(getMappingsResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final GetMappingsRequest request) {
        // RestGetMappingAction
        final CurlRequest curlRequest = client.getCurlRequest(GET, "/_mapping/" + String.join(",", request.types()), request.indices());
        curlRequest.param("local", Boolean.toString(request.local()));
        return curlRequest;
    }

    protected GetMappingsResponse getGetMappingsResponse(final XContentParser parser, final Supplier<GetMappingsResponse> newResponse)
            throws IOException {
        final ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> indexMapBuilder = ImmutableOpenMap.builder();
        String index = null;
        Token token = parser.nextToken();
        if (token != null) {
            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                if (token == Token.FIELD_NAME) {
                    index = parser.currentName();
                } else if (token == Token.START_OBJECT) {
                    while (parser.nextToken() == Token.FIELD_NAME) {
                        if (MAPPINGS_FIELD.match(parser.currentName(), LoggingDeprecationHandler.INSTANCE)) {
                            indexMapBuilder.put(index, getMappings(parser));
                            break;
                        } else {
                            parser.skipChildren();
                        }
                    }
                }
            }
        }

        final ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = indexMapBuilder.build();

        try (ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
            out.writeVInt(mappings.size());
            for (final ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> indexEntry : mappings) {
                out.writeString(indexEntry.key);
                out.writeVInt(indexEntry.value.size());
                for (final ObjectObjectCursor<String, MappingMetaData> typeEntry : indexEntry.value) {
                    out.writeString(typeEntry.key);
                    typeEntry.value.writeTo(out);
                }
            }

            final GetMappingsResponse response = newResponse.get();
            response.readFrom(out.toStreamInput());
            return response;
        }
    }

    public static ImmutableOpenMap<String, MappingMetaData> getMappings(final XContentParser parser) throws IOException {
        final ImmutableOpenMap.Builder<String, MappingMetaData> mappingsBuilder = ImmutableOpenMap.builder();
        String type = null;
        Token token = parser.nextToken();
        if (token == null) {
            return mappingsBuilder.build();
        }
        while ((token = parser.nextToken()) != Token.END_OBJECT) {
            if (token == Token.FIELD_NAME) {
                type = parser.currentName();
            } else if (token == Token.START_OBJECT) {
                final Map<String, Object> mapping = parser.mapOrdered();
                mappingsBuilder.put(type, new MappingMetaData(type, mapping));
            }
        }
        return mappingsBuilder.build();
    }
}
