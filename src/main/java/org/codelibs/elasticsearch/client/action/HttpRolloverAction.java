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

import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

public class HttpRolloverAction extends HttpAction {

    protected final RolloverAction action;

    public HttpRolloverAction(final HttpClient client, final RolloverAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final RolloverRequest request, final ActionListener<RolloverResponse> listener) {
        String source = null;
        try {
            final XContentBuilder builder = request.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS);
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to parse a request.", e);
        }
        client.getCurlRequest(POST, "/_rollover" + (request.getNewIndexName() != null ? "/" + request.getNewIndexName() : ""),
                request.getAlias()).param("dry_run", (request.isDryRun() ? "" : null)).body(source).execute(response -> {
            if (response.getHttpStatusCode() != 200) {
                throw new ElasticsearchException("error: " + response.getHttpStatusCode());
            }
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final RolloverResponse rolloverResponse = RolloverResponse.fromXContent(parser);
                listener.onResponse(rolloverResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }
}
