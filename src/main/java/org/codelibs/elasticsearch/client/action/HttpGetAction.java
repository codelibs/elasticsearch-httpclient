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
import java.util.Locale;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.VersionType;

public class HttpGetAction extends HttpAction {

    protected final GetAction action;

    public HttpGetAction(final HttpClient client, final GetAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetRequest request, final ActionListener<GetResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final GetResponse getResponse = GetResponse.fromXContent(parser);
                listener.onResponse(getResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }

    private CurlRequest getCurlRequest(final GetRequest request) {
        // RestGetAction
        final CurlRequest curlRequest = client.getCurlRequest(GET, "/" + request.type() + "/" + request.id(), request.index());
        if (request.refresh()) {
            curlRequest.param("refresh", "true");
        }
        if (request.routing() != null) {
            curlRequest.param("routing", request.routing());
        }
        if (request.parent() != null) {
            curlRequest.param("parent", request.parent());
        }
        if (request.preference() != null) {
            curlRequest.param("preference", request.preference());
        }
        if (request.realtime()) {
            curlRequest.param("realtime", "true");
        }
        if (request.storedFields() != null) {
            curlRequest.param("stored_fields", String.join(",", request.storedFields()));
        }
        if (request.version() >= 0) {
            curlRequest.param("version", Long.toString(request.version()));
        }
        if (!VersionType.INTERNAL.equals(request.versionType())) {
            curlRequest.param("version_type", request.versionType().name().toLowerCase(Locale.ROOT));
        }
        return curlRequest;
    }
}
