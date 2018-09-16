package org.codelibs.elasticsearch.client.action;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpCreateSnapshotAction extends HttpAction {

    protected final CreateSnapshotAction action;

    public HttpCreateSnapshotAction(final HttpClient client, final CreateSnapshotAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final CreateSnapshotRequest request, final ActionListener<CreateSnapshotResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final CreateSnapshotResponse cancelTasksResponse = CreateSnapshotResponse.fromXContent(parser);
                listener.onResponse(cancelTasksResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final CreateSnapshotRequest request) {
        // RestCreateSnapshotAction
        final StringBuilder pathBuf = new StringBuilder(100).append("/_snapshot");
        if (request.repository() != null) {
            pathBuf.append('/').append(request.repository());
        }
        if (request.snapshot() != null) {
            pathBuf.append('/').append(request.snapshot());
        }
        final CurlRequest curlRequest = client.getCurlRequest(POST, pathBuf.toString());
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        curlRequest.param("wait_for_completion", String.valueOf(request.waitForCompletion()));
        return curlRequest;
    }
}
