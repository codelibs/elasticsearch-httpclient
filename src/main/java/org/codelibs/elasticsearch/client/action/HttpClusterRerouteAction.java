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
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpClusterRerouteAction extends HttpAction {

    protected final ClusterRerouteAction action;

    public HttpClusterRerouteAction(final HttpClient client, final ClusterRerouteAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final ClusterRerouteRequest request, final ActionListener<AcknowledgedResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final AcknowledgedResponse clusterRerouteResponse = ClusterRerouteResponse.fromXContent(parser);
                listener.onResponse(clusterRerouteResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final ClusterRerouteRequest request) {
        // RestClusterRerouteAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_cluster/reroute");
        if (request.dryRun()) {
            curlRequest.param("dry_run", Boolean.toString(request.dryRun()));
        }
        if (request.explain()) {
            curlRequest.param("explain", Boolean.toString(request.explain()));
        }
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        if (request.isRetryFailed()) {
            curlRequest.param("retry_failed", Boolean.toString(request.isRetryFailed()));
        }
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }
}
