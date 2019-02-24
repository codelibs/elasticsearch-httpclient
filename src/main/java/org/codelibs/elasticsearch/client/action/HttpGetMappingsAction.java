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
import java.util.Map;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexNotFoundException;

public class HttpGetMappingsAction extends HttpAction {

    protected final GetMappingsAction action;

    private static final ParseField MAPPINGS = new ParseField("mappings");

    public HttpGetMappingsAction(final HttpClient client, final GetMappingsAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetMappingsRequest request, final ActionListener<GetMappingsResponse> listener) {
        getCurlRequest(request).execute(response -> {
            if (response.getHttpStatusCode() == 404) {
                throw new IndexNotFoundException(String.join(",", request.indices()));
            }
            try (final XContentParser parser = createParser(response)) {
                final GetMappingsResponse getMappingsResponse = /*GetMappingsResponse.*/fromXContent(parser);
                listener.onResponse(getMappingsResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final GetMappingsRequest request) {
        // RestGetMappingAction
        final CurlRequest curlRequest = client.getCurlRequest(GET, "/_mapping", request.indices());
        curlRequest.param("local", Boolean.toString(request.local()));
        return curlRequest;
    }

    // TODO replace with GetMappingsResonse#fromXContent, but it cannot parse dynamic_templates in 7.0.0-beta1.
    // from GetMappingsResonse
    public static GetMappingsResponse fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        assert parser.currentToken() == XContentParser.Token.START_OBJECT;
        Map<String, Object> parts = parser.map();

        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> builder = new ImmutableOpenMap.Builder<>();
        for (Map.Entry<String, Object> entry : parts.entrySet()) {
            final String indexName = entry.getKey();
            assert entry.getValue() instanceof Map : "expected a map as type mapping, but got: " + entry.getValue().getClass();
            final Map<String, Object> mapping = (Map<String, Object>) ((Map) entry.getValue()).get(MAPPINGS.getPreferredName());

            ImmutableOpenMap.Builder<String, MappingMetaData> typeBuilder = new ImmutableOpenMap.Builder<>();
            for (Map.Entry<String, Object> typeEntry : mapping.entrySet()) {
                final String typeName = typeEntry.getKey();
                assert typeEntry.getValue() instanceof Map : "expected a map as inner type mapping, but got: "
                        + typeEntry.getValue().getClass();
                if ("dynamic_templates".equals(typeName)) {
                    continue;
                }
                final Map<String, Object> fieldMappings = (Map<String, Object>) typeEntry.getValue();
                MappingMetaData mmd = new MappingMetaData(typeName, fieldMappings);
                typeBuilder.put(typeName, mmd);
            }
            builder.put(indexName, typeBuilder.build());
        }

        return new GetMappingsResponse(builder.build());
    }

}
