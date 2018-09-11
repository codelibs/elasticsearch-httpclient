package org.codelibs.elasticsearch.client.action;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpListTasksAction extends HttpAction {

    protected final ListTasksAction action;

    public HttpListTasksAction(final HttpClient client, final ListTasksAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final ListTasksRequest request, final ActionListener<ListTasksResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final ListTasksResponse listTasksResponse = ListTasksResponse.fromXContent(parser);
                listener.onResponse(listTasksResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final ListTasksRequest request) {
        // RestListTasksAction
        final CurlRequest curlRequest = client.getCurlRequest(GET, "/_tasks");
        curlRequest.param("detailed", String.valueOf(request.getDetailed()));
        curlRequest.param("parent_task_id", String.valueOf(request.getParentTaskId()));
        curlRequest.param("wait_for_completion", String.valueOf(request.getWaitForCompletion()));
        if (request.getNodes() != null) {
            curlRequest.param("nodes", String.join(",", request.getNodes()));
        }
        if (request.getActions() != null) {
            curlRequest.param("actions", String.join(",", request.getActions()));
        }
        return curlRequest;
    }
}
