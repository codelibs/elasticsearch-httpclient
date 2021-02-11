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

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.codelibs.elasticsearch.client.util.UrlUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpGetFieldMappingsAction extends HttpAction {

    protected final GetFieldMappingsAction action;

    public HttpGetFieldMappingsAction(final HttpClient client, final GetFieldMappingsAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetFieldMappingsRequest request, final ActionListener<GetFieldMappingsResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final GetFieldMappingsResponse getFieldMappingsResponse = fromXContent(parser);
                listener.onResponse(getFieldMappingsResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final GetFieldMappingsRequest request) {
        // RestGetFieldMappingsAction
        final StringBuilder pathSuffix = new StringBuilder(100).append("/_mapping/field/");
        if (request.fields().length > 0) {
            pathSuffix.append(UrlUtils.joinAndEncode(",", request.fields()));
        }
        final CurlRequest curlRequest = client.getCurlRequest(GET, pathSuffix.toString(), request.indices());
        curlRequest.param("include_defaults", Boolean.toString(request.includeDefaults()));
        curlRequest.param("local", Boolean.toString(request.local()));
        return curlRequest;
    }

    protected GetFieldMappingsResponse fromXContent(final XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        final Map<String, Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>>> mappings = new HashMap<>();
        if (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
            while (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                final String index = parser.currentName();
                final Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>> typeMappings =
                        parseTypeMappings(parser, index);
                mappings.put(index, typeMappings);

                parser.nextToken();
            }
        }

        return newGetFieldMappingsResponse(mappings);
    }

    protected Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>> parseTypeMappings(final XContentParser parser,
            final String index) throws IOException {
        final ObjectParser<Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>>, String> objectParser =
                new ObjectParser<>(MAPPINGS_FIELD.getPreferredName(), true, HashMap::new);
        objectParser.declareField((p, typeMappings, idx) -> {
            p.nextToken();
            while (p.currentToken() == XContentParser.Token.FIELD_NAME) {
                final String typeName = p.currentName();

                if (p.nextToken() == XContentParser.Token.START_OBJECT) {
                    final Map<String, GetFieldMappingsResponse.FieldMappingMetadata> typeMapping = new HashMap<>();
                    typeMappings.put(typeName, typeMapping);
                    do {
                        final String fieldName = p.currentName();
                        final GetFieldMappingsResponse.FieldMappingMetadata fieldMappingMetaData = getFieldMappingMetadata(p);
                        typeMapping.put(fieldName, fieldMappingMetaData);
                    } while (p.nextToken() == XContentParser.Token.START_OBJECT);
                } else {
                    p.skipChildren();
                }
                p.nextToken();
            }
        }, MAPPINGS_FIELD, ObjectParser.ValueType.OBJECT);

        return objectParser.parse(parser, index);
    }

    protected GetFieldMappingsResponse.FieldMappingMetadata getFieldMappingMetadata(final XContentParser parser) throws IOException {
        final ConstructingObjectParser<GetFieldMappingsResponse.FieldMappingMetadata, String> objectParser =
                new ConstructingObjectParser<>("field_mapping_meta_data", true,
                        a -> new GetFieldMappingsResponse.FieldMappingMetadata((String) a[0], (BytesReference) a[1]));

        objectParser.declareField(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.text(), FULL_NAME_FIELD,
                ObjectParser.ValueType.STRING);
        objectParser.declareField(ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> BytesReference.bytes(XContentFactory.jsonBuilder().copyCurrentStructure(p)), MAPPING_FIELD,
                ObjectParser.ValueType.OBJECT);

        return objectParser.parse(parser, null);
    }

    protected GetFieldMappingsResponse newGetFieldMappingsResponse(
            final Map<String, Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>>> mappings) {
        final Class<GetFieldMappingsResponse> clazz = GetFieldMappingsResponse.class;
        final Class<?>[] types = { Map.class };
        try {
            final Constructor<GetFieldMappingsResponse> constructor = clazz.getDeclaredConstructor(types);
            constructor.setAccessible(true);
            return constructor.newInstance(mappings);
        } catch (final Exception e) {
            throw new ElasticsearchException("Failed to create GetFieldMappingsResponse.", e);
        }
    }
}
