package org.codelibs.elasticsearch.client.action;

import java.io.IOException;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.codelibs.elasticsearch.client.util.UrlUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

public class HttpRestoreSnapshotAction extends HttpAction {

    protected final RestoreSnapshotAction action;

    public HttpRestoreSnapshotAction(final HttpClient client, final RestoreSnapshotAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final RestoreSnapshotRequest request, final ActionListener<RestoreSnapshotResponse> listener) {
        String source = null;
        try (final XContentBuilder builder = request.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)) {
            builder.flush();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to parse a request.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final RestoreSnapshotResponse restoreSnapshotResponse = RestoreSnapshotResponse.fromXContent(parser);
                listener.onResponse(restoreSnapshotResponse);
            } catch (final Throwable t) {
                listener.onFailure(toElasticsearchException(response, t));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final RestoreSnapshotRequest request) {
        // RestRestoreSnapshotAction
        final StringBuilder pathBuf = new StringBuilder(100).append("/_snapshot");
        if (request.repository() != null) {
            pathBuf.append('/').append(UrlUtils.encode(request.repository()));
        }
        if (request.snapshot() != null) {
            pathBuf.append('/').append(UrlUtils.encode(request.snapshot()));
        }
        pathBuf.append('/').append("_restore");
        final CurlRequest curlRequest = client.getCurlRequest(POST, pathBuf.toString());
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        if (request.waitForCompletion()) {
            curlRequest.param("wait_for_completion", Boolean.toString(request.waitForCompletion()));
        }
        return curlRequest;
    }

}
