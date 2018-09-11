package org.codelibs.elasticsearch.client.action;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpGetIndexTemplatesAction extends HttpAction {

    protected final GetIndexTemplatesAction action;

    public HttpGetIndexTemplatesAction(final HttpClient client, final GetIndexTemplatesAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetIndexTemplatesRequest request, final ActionListener<GetIndexTemplatesResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final GetIndexTemplatesResponse getIndexTemplatesResponse = GetIndexTemplatesResponse.fromXContent(parser);
                listener.onResponse(getIndexTemplatesResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final GetIndexTemplatesRequest request) {
        // RestGetIndexTemplateAction
        final CurlRequest curlRequest = client.getCurlRequest(GET, "/_template/" + String.join(",", request.names()));
        curlRequest.param("local", Boolean.toString(request.local()));
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }
}
