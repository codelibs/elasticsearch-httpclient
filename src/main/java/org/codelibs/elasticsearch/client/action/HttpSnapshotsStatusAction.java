package org.codelibs.elasticsearch.client.action;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
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
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final SnapshotsStatusRequest request) {
        // RestSnapshotsStatusAction
        final StringBuilder pathBuf = new StringBuilder(100).append("/_snapshot");
        if (request.repository() != null) {
            pathBuf.append('/').append(request.repository());
        }
        if (request.snapshots() != null && request.snapshots().length > 0) {
            pathBuf.append('/').append(String.join(",", request.snapshots()));
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
