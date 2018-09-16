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

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpCancelTasksAction extends HttpAction {

    protected final CancelTasksAction action;

    public HttpCancelTasksAction(final HttpClient client, final CancelTasksAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final CancelTasksRequest request, final ActionListener<CancelTasksResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final CancelTasksResponse cancelTasksResponse = CancelTasksResponse.fromXContent(parser);
                listener.onResponse(cancelTasksResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final CancelTasksRequest request) {
        // RestCancelTasksAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_tasks/_cancel");
        curlRequest.param("task_id", String.valueOf(request.getTaskId()));
        curlRequest.param("parent_task_id", String.valueOf(request.getParentTaskId()));
        if (request.getNodes() != null) {
            curlRequest.param("nodes", String.join(",", request.getNodes()));
        }
        if (request.getActions() != null) {
            curlRequest.param("actions", String.join(",", request.getActions()));
        }
        return curlRequest;
    }
}
