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
import java.util.HashMap;
import java.util.Map;

import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;

public class HttpGetSettingsAction extends HttpAction {

    protected final GetSettingsAction action;

    public HttpGetSettingsAction(final HttpClient client, final GetSettingsAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetSettingsRequest request, final ActionListener<GetSettingsResponse> listener) {
        client.getCurlRequest(GET, "/_settings", request.indices()).execute(response -> {
            if (response.getHttpStatusCode() != 200) {
                throw new ElasticsearchException("error: " + response.getHttpStatusCode());
            }
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final GetSettingsResponse getSettingsResponse = getGetSettingsResponse(parser);
                listener.onResponse(getSettingsResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }

    protected GetSettingsResponse getGetSettingsResponse(final XContentParser parser) throws IOException {
        final HashMap<String, Settings> indexToSettings = new HashMap<>();

        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        parser.nextToken();

        while (!parser.isClosed()) {
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                parseIndexEntry(parser, indexToSettings);
            } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                parser.skipChildren();
            } else {
                parser.nextToken();
            }
        }

        final ImmutableOpenMap<String, Settings> settingsMap =
                ImmutableOpenMap.<String, Settings> builder().putAll(indexToSettings).build();

        return new GetSettingsResponse(settingsMap);
    }

    protected void parseIndexEntry(final XContentParser parser, final Map<String, Settings> indexToSettings) throws IOException {
        final String indexName = parser.currentName();
        parser.nextToken();
        while (!parser.isClosed() && parser.currentToken() != XContentParser.Token.END_OBJECT) {
            parseSettingsField(parser, indexName, indexToSettings);
        }
    }

    protected void parseSettingsField(final XContentParser parser, final String currentIndexName,
            final Map<String, Settings> indexToSettings) throws IOException {
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            if (SETTINGS_FIELD.match(parser.currentName(), LoggingDeprecationHandler.INSTANCE)) {
                indexToSettings.put(currentIndexName, Settings.fromXContent(parser));
            } else {
                parser.skipChildren();
            }
        } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
            parser.skipChildren();
        }
        parser.nextToken();
    }
}
