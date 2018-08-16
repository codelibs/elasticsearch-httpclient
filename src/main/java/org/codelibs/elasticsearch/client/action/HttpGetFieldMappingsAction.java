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

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetaData;
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
        final StringBuilder pathSuffix = new StringBuilder(100);
        if (request.types().length > 0) {
            pathSuffix.append(String.join(",", request.types())).append('/');
        }
        pathSuffix.append("field/");
        if (request.fields().length > 0) {
            pathSuffix.append(String.join(",", request.fields()));
        }
        client.getCurlRequest(GET, "/_mapping/" + pathSuffix.toString(), request.indices())
                .param("include_defaults", String.valueOf(request.includeDefaults())).execute(response -> {
                    if (response.getHttpStatusCode() != 200) {
                        throw new ElasticsearchException("error: " + response.getHttpStatusCode());
                    }
                    try (final InputStream in = response.getContentAsStream()) {
                        final XContentParser parser = createParser(in);
                        final GetFieldMappingsResponse getFieldMappingsResponse = getGetFieldMappingsResponse(parser, action::newResponse);
                        listener.onResponse(getFieldMappingsResponse);
                    } catch (final Exception e) {
                        listener.onFailure(e);
                    }
                }, listener::onFailure);
    }

    protected GetFieldMappingsResponse getGetFieldMappingsResponse(final XContentParser parser,
            final Supplier<GetFieldMappingsResponse> newResponse) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);

        final Map<String, Map<String, Map<String, FieldMappingMetaData>>> mappings = new HashMap<>();
        if (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
            while (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                final String index = parser.currentName();

                final Map<String, Map<String, FieldMappingMetaData>> typeMappings = parseTypeMappings(parser, index);
                mappings.put(index, typeMappings);

                parser.nextToken();
            }
        }

        return newGetFieldMappingsResponse(mappings);
    }

    protected Map<String, Map<String, FieldMappingMetaData>> parseTypeMappings(final XContentParser parser, final String index)
            throws IOException {
        final ObjectParser<Map<String, Map<String, FieldMappingMetaData>>, String> objectParser =
                new ObjectParser<>(MAPPINGS_FIELD.getPreferredName(), true, HashMap::new);

        objectParser.declareField((p, typeMappings, idx) -> {
            p.nextToken();
            while (p.currentToken() == XContentParser.Token.FIELD_NAME) {
                final String typeName = p.currentName();

                if (p.nextToken() == XContentParser.Token.START_OBJECT) {
                    final Map<String, FieldMappingMetaData> typeMapping = new HashMap<>();
                    typeMappings.put(typeName, typeMapping);

                    while (p.nextToken() == XContentParser.Token.FIELD_NAME) {
                        final String fieldName = p.currentName();
                        final FieldMappingMetaData fieldMappingMetaData = getFieldMappingMetaData(p);
                        typeMapping.put(fieldName, fieldMappingMetaData);
                    }
                } else {
                    p.skipChildren();
                }
                p.nextToken();
            }
        }, MAPPINGS_FIELD, ObjectParser.ValueType.OBJECT);

        return objectParser.parse(parser, index);
    }

    protected FieldMappingMetaData getFieldMappingMetaData(final XContentParser parser) throws IOException {
        final ConstructingObjectParser<FieldMappingMetaData, String> objectParser =
                new ConstructingObjectParser<>("field_mapping_meta_data", true, a -> new FieldMappingMetaData((String) a[0],
                        (BytesReference) a[1]));

        objectParser.declareField(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.text(), FULL_NAME_FIELD,
                ObjectParser.ValueType.STRING);
        objectParser.declareField(ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> BytesReference.bytes(XContentFactory.jsonBuilder().copyCurrentStructure(p)), MAPPING_FIELD,
                ObjectParser.ValueType.OBJECT);

        return objectParser.parse(parser, null);
    }

    protected GetFieldMappingsResponse newGetFieldMappingsResponse(Map<String, Map<String, Map<String, FieldMappingMetaData>>> mappings) {
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
