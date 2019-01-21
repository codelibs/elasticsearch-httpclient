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

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.codelibs.elasticsearch.client.util.UrlUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpDeleteIndexTemplateAction extends HttpAction {

    protected final DeleteIndexTemplateAction action;

    public HttpDeleteIndexTemplateAction(final HttpClient client, final DeleteIndexTemplateAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final DeleteIndexTemplateRequest request, final ActionListener<AcknowledgedResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final AcknowledgedResponse deleteIndexTemplateResponse = getAcknowledgedResponse(parser, action::newResponse);
                listener.onResponse(deleteIndexTemplateResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final DeleteIndexTemplateRequest request) {
        // RestDeleteIndexTemplatesAction
        final CurlRequest curlRequest = client.getCurlRequest(DELETE, "/_template/" + UrlUtils.encode(request.name()));
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }
}
