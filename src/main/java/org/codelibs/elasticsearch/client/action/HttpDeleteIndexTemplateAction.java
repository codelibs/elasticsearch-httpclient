package org.codelibs.elasticsearch.client.action;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpDeleteIndexTemplateAction extends HttpAction {

    protected final DeleteIndexTemplateAction action;

    public HttpDeleteIndexTemplateAction(final HttpClient client, final DeleteIndexTemplateAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final DeleteIndexTemplateRequest request, final ActionListener<DeleteIndexTemplateResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final DeleteIndexTemplateResponse deleteIndexTemplateResponse = getAcknowledgedResponse(parser, action::newResponse);
                listener.onResponse(deleteIndexTemplateResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final DeleteIndexTemplateRequest request) {
        // RestDeleteIndexTemplatesAction
        final CurlRequest curlRequest = client.getCurlRequest(DELETE, "/_template/" + request.name());
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }
}
