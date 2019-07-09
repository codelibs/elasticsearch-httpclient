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
