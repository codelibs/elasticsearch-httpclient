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

import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptAction;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpDeleteStoredScriptAction extends HttpAction {

    protected final DeleteStoredScriptAction action;

    public HttpDeleteStoredScriptAction(final HttpClient client, final DeleteStoredScriptAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final DeleteStoredScriptRequest request, final ActionListener<DeleteStoredScriptResponse> listener) {
        client.getCurlRequest(DELETE, "/_scripts/" + request.id())
                .param("timeout", (request.timeout() == null ? null : request.timeout().toString())).execute(response -> {
                    try (final InputStream in = response.getContentAsStream()) {
                        final XContentParser parser = createParser(in);
                        final DeleteStoredScriptResponse deleteStoredScriptResponse = getAcknowledgedResponse(parser, action::newResponse);
                        listener.onResponse(deleteStoredScriptResponse);
                    } catch (final Exception e) {
                        listener.onFailure(e);
                    }
                }, listener::onFailure);
    }
}