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
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesAction;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;

public class HttpFieldCapabilitiesAction extends HttpAction {

    protected final FieldCapabilitiesAction action;

    public HttpFieldCapabilitiesAction(final HttpClient client, final FieldCapabilitiesAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final FieldCapabilitiesRequest request, final ActionListener<FieldCapabilitiesResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final FieldCapabilitiesResponse fieldCapabilitiesResponse = getFieldCapabilitiesResponse(parser);
                listener.onResponse(fieldCapabilitiesResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final FieldCapabilitiesRequest request) {
        // RestFieldCapabilitiesAction
        final CurlRequest curlRequest = client.getCurlRequest(GET, "/_field_caps", request.indices());
        if (request.fields() != null) {
            curlRequest.param("fields", String.join(",", request.fields()));
        }
        return curlRequest;
    }

    protected FieldCapabilitiesResponse getFieldCapabilitiesResponse(final XContentParser parser) {
        // workaround fix
        @SuppressWarnings("unchecked")
        final ConstructingObjectParser<FieldCapabilitiesResponse, Void> objectParser =
                new ConstructingObjectParser<>("field_capabilities_response", true,
                        a -> newFieldCapabilitiesResponse(((List<Tuple<String, Map<String, FieldCapabilities>>>) a[0]).stream().collect(
                                Collectors.toMap(Tuple::v1, Tuple::v2))));

        objectParser.declareNamedObjects(ConstructingObjectParser.constructorArg(), (p, c, n) -> {
            final Map<String, FieldCapabilities> typeToCapabilities = parseTypeToCapabilities(p, n);
            return new Tuple<>(n, typeToCapabilities);
        }, FIELDS_FIELD);

        try {
            return objectParser.parse(parser, null);
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to parse FieldCapabilitiesResponse.", e);
        }
    }

    protected FieldCapabilitiesResponse newFieldCapabilitiesResponse(final Map<String, Map<String, FieldCapabilities>> map) {
        final Class<FieldCapabilitiesResponse> clazz = FieldCapabilitiesResponse.class;
        final Class<?>[] types = { Map.class };
        try {
            final Constructor<FieldCapabilitiesResponse> constructor = clazz.getDeclaredConstructor(types);
            constructor.setAccessible(true);
            return constructor.newInstance(map);
        } catch (final Exception e) {
            throw new ElasticsearchException("Failed to create FieldCapabilitiesResponse.", e);
        }
    }

    protected Map<String, FieldCapabilities> parseTypeToCapabilities(final XContentParser parser, final String name) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);

        final Map<String, FieldCapabilities> typeToCapabilities = new HashMap<>();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);
            final String type = parser.currentName();
            final FieldCapabilities capabilities = getFieldCapabilitiesfromXContent(name, parser);
            typeToCapabilities.put(type, capabilities);
        }
        return typeToCapabilities;
    }

    protected FieldCapabilities getFieldCapabilitiesfromXContent(final String sname, final XContentParser parser) throws IOException {
        @SuppressWarnings("unchecked")
        final ConstructingObjectParser<FieldCapabilities, String> objectParser =
                new ConstructingObjectParser<>("field_capabilities", true, (a, name) -> newFieldCapabilities(name, (String) a[0], true,
                        true, (a[3] != null ? ((List<String>) a[3]).toArray(new String[0]) : null),
                        (a[4] != null ? ((List<String>) a[4]).toArray(new String[0]) : null),
                        (a[5] != null ? ((List<String>) a[5]).toArray(new String[0]) : null)));

        objectParser.declareString(ConstructingObjectParser.constructorArg(), TYPE_FIELD);
        objectParser.declareBoolean(ConstructingObjectParser.constructorArg(), SEARCHABLE_FIELD);
        objectParser.declareBoolean(ConstructingObjectParser.constructorArg(), AGGREGATABLE_FIELD);
        objectParser.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), INDICES_FIELD);
        objectParser.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), NON_SEARCHABLE_INDICES_FIELD);
        objectParser.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), NON_AGGREGATABLE_INDICES_FIELD);

        return objectParser.parse(parser, sname);
    }

    protected FieldCapabilities newFieldCapabilities(final String name, final String type, final boolean isSearchable,
            final boolean isAggregatable, final String[] indices, final String[] nonSearchableIndices, final String[] nonAggregatableIndices) {
        final Class<FieldCapabilities> clazz = FieldCapabilities.class;
        final Class<?>[] types =
                { String.class, String.class, boolean.class, boolean.class, String[].class, String[].class, String[].class };
        try {
            final Constructor<FieldCapabilities> constructor = clazz.getDeclaredConstructor(types);
            constructor.setAccessible(true);
            return constructor.newInstance(name, type, isSearchable, isAggregatable, indices, nonSearchableIndices, nonAggregatableIndices);
        } catch (final Exception e) {
            throw new ElasticsearchException("Failed to create ConstructingObjectParser.", e);
        }
    }
}
