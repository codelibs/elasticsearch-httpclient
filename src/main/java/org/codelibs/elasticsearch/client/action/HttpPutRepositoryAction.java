package org.codelibs.elasticsearch.client.action;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpPutRepositoryAction extends HttpAction {

    protected final PutRepositoryAction action;

    public HttpPutRepositoryAction(final HttpClient client, final PutRepositoryAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final PutRepositoryRequest request, final ActionListener<PutRepositoryResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final PutRepositoryResponse putRepositoryResponse = PutRepositoryResponse.fromXContent(parser);
                listener.onResponse(putRepositoryResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final PutRepositoryRequest request) {
        // RestPutRepositoryAction
        final CurlRequest curlRequest = client.getCurlRequest(PUT, "/_snapshot/" + request.name());
        curlRequest.param("verify", Boolean.toString(request.verify()));
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        return curlRequest;
    }
}
