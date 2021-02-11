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

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesAction;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpGetRepositoriesAction extends HttpAction {

    protected final GetRepositoriesAction action;

    public HttpGetRepositoriesAction(final HttpClient client, final GetRepositoriesAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetRepositoriesRequest request, final ActionListener<GetRepositoriesResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final GetRepositoriesResponse getRepositoriesResponse = GetRepositoriesResponse.fromXContent(parser);
                listener.onResponse(getRepositoriesResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final GetRepositoriesRequest request) {
        // RestGetRepositoriesAction
        final CurlRequest curlRequest = client.getCurlRequest(GET, "/_snapshot");
        curlRequest.param("local", Boolean.toString(request.local()));
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        if (request.repositories() != null) {
            curlRequest.param("repository", String.join(",", request.repositories()));
        }
        return curlRequest;
    }
}
