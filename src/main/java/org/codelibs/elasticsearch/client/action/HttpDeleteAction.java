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
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpDeleteAction extends HttpAction {

    protected final DeleteAction action;

    public HttpDeleteAction(final HttpClient client, final DeleteAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final DeleteRequest request, final ActionListener<DeleteResponse> listener) {
        client.getCurlRequest(DELETE, "/" + request.type() + "/" + request.id(), request.index()).param("routing", request.routing())
                .param("version", String.valueOf(request.version())).execute(response -> {
            try (final InputStream in = response.getContentAsStream()) {
                if (response.getHttpStatusCode() != 200) {
                    throw new ElasticsearchException("error: " + response.getHttpStatusCode());
                }
                final XContentParser parser = createParser(in);
                final DeleteResponse deleteResponse = DeleteResponse.fromXContent(parser);
                listener.onResponse(deleteResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }
}
