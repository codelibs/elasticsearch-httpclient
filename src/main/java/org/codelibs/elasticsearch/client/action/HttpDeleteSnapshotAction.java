package org.codelibs.elasticsearch.client.action;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpDeleteSnapshotAction extends HttpAction {

    protected final DeleteSnapshotAction action;

    public HttpDeleteSnapshotAction(final HttpClient client, final DeleteSnapshotAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final DeleteSnapshotRequest request, final ActionListener<DeleteSnapshotResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final DeleteSnapshotResponse cancelTasksResponse = DeleteSnapshotResponse.fromXContent(parser);
                listener.onResponse(cancelTasksResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final DeleteSnapshotRequest request) {
        // RestDeleteSnapshotAction
        final StringBuilder pathBuf = new StringBuilder(100).append("/_snapshot");
        if (request.repository() != null) {
            pathBuf.append('/').append(request.repository());
        }
        if (request.snapshot() != null) {
            pathBuf.append('/').append(request.snapshot());
        }
        final CurlRequest curlRequest = client.getCurlRequest(DELETE, pathBuf.toString());
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }
}
