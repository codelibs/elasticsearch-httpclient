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
import java.lang.reflect.Constructor;

import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptAction;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.StoredScriptSource;

public class HttpGetStoredScriptAction extends HttpAction {

    protected final GetStoredScriptAction action;

    public HttpGetStoredScriptAction(final HttpClient client, final GetStoredScriptAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetStoredScriptRequest request, final ActionListener<GetStoredScriptResponse> listener) {
        client.getCurlRequest(GET, "/_scripts/" + request.id()).execute(response -> {
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final GetStoredScriptResponse getStoredScriptResponse = getGetStoredScriptResponse(parser);
                listener.onResponse(getStoredScriptResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }

    protected GetStoredScriptResponse getGetStoredScriptResponse(final XContentParser parser) {
        @SuppressWarnings("unchecked")
        final ConstructingObjectParser<GetStoredScriptResponse, String> objectParser =
                new ConstructingObjectParser<>("get_stored_script_response", true,
                        (a, c) -> newGetStoredScriptResponse((StoredScriptSource) a[0]));

        objectParser.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> StoredScriptSource.fromXContent(p),
                SCRIPT_FIELD);

        return objectParser.apply(parser, null);
    }

    protected GetStoredScriptResponse newGetStoredScriptResponse(final StoredScriptSource storedScriptSource) {
        final Class<GetStoredScriptResponse> clazz = GetStoredScriptResponse.class;
        final Class<?>[] types = { StoredScriptSource.class };
        try {
            final Constructor<GetStoredScriptResponse> constructor = clazz.getDeclaredConstructor(types);
            constructor.setAccessible(true);
            return constructor.newInstance(storedScriptSource);
        } catch (final Exception e) {
            throw new ElasticsearchException("Failed to create FieldCapabilitiesResponse.", e);
        }
    }
}
