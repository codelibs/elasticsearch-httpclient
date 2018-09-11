package org.codelibs.elasticsearch.client.action;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpVerifyRepositoryAction extends HttpAction {

    protected final VerifyRepositoryAction action;

    public HttpVerifyRepositoryAction(final HttpClient client, final VerifyRepositoryAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final VerifyRepositoryRequest request, final ActionListener<VerifyRepositoryResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final VerifyRepositoryResponse verifyRepositoryResponse = VerifyRepositoryResponse.fromXContent(parser);
                listener.onResponse(verifyRepositoryResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final VerifyRepositoryRequest request) {
        // RestVerifyRepositoryAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_snapshot/" + request.name() + "/_verify");
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        return curlRequest;
    }
}
