package org.codelibs.elasticsearch.client.action;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpGetSnapshotsAction extends HttpAction {

    protected final GetSnapshotsAction action;

    public HttpGetSnapshotsAction(final HttpClient client, final GetSnapshotsAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetSnapshotsRequest request, final ActionListener<GetSnapshotsResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final GetSnapshotsResponse cancelTasksResponse = GetSnapshotsResponse.fromXContent(parser);
                listener.onResponse(cancelTasksResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final GetSnapshotsRequest request) {
        // RestGetSnapshotsAction
        final StringBuilder pathBuf = new StringBuilder(100).append("/_snapshot");
        if (request.repository() != null) {
            pathBuf.append('/').append(request.repository());
        } else {
            pathBuf.append("_all");
        }
        if (request.snapshots() != null && request.snapshots().length > 0) {
            pathBuf.append('/').append(String.join(",", request.snapshots()));
        } else {
            pathBuf.append("_all");
        }
        final CurlRequest curlRequest = client.getCurlRequest(GET, pathBuf.toString());
        curlRequest.param("ignore_unavailable", String.valueOf(request.ignoreUnavailable()));
        curlRequest.param("verbose", String.valueOf(request.verbose()));
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }
}