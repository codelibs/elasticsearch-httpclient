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
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusAction;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpSnapshotsStatusAction extends HttpAction {

    protected final SnapshotsStatusAction action;

    public HttpSnapshotsStatusAction(final HttpClient client, final SnapshotsStatusAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final SnapshotsStatusRequest request, final ActionListener<SnapshotsStatusResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final SnapshotsStatusResponse cancelTasksResponse = SnapshotsStatusResponse.fromXContent(parser);
                listener.onResponse(cancelTasksResponse);
            } catch (final Throwable t) {
                listener.onFailure(toElasticsearchException(response, t));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final SnapshotsStatusRequest request) {
        // RestSnapshotsStatusAction
        final StringBuilder pathBuf = new StringBuilder(100).append("/_snapshot");
        if (request.repository() != null) {
            pathBuf.append('/').append(UrlUtils.encode(request.repository()));
        }
        if (request.snapshots() != null && request.snapshots().length > 0) {
            pathBuf.append('/').append(UrlUtils.joinAndEncode(",", request.snapshots()));
        }
        pathBuf.append("/_status");
        final CurlRequest curlRequest = client.getCurlRequest(GET, pathBuf.toString());
        curlRequest.param("ignore_unavailable", String.valueOf(request.ignoreUnavailable()));
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }
}
