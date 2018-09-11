package org.codelibs.elasticsearch.client.action;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpDeleteRepositoryAction extends HttpAction {

    protected final DeleteRepositoryAction action;

    public HttpDeleteRepositoryAction(final HttpClient client, final DeleteRepositoryAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final DeleteRepositoryRequest request, final ActionListener<DeleteRepositoryResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final DeleteRepositoryResponse deleteRepositoryResponse = DeleteRepositoryResponse.fromXContent(parser);
                listener.onResponse(deleteRepositoryResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final DeleteRepositoryRequest request) {
        // RestVerifyRepositoryAction
        final CurlRequest curlRequest = client.getCurlRequest(DELETE, "/_snapshot/" + request.name());
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        return curlRequest;
    }
}
