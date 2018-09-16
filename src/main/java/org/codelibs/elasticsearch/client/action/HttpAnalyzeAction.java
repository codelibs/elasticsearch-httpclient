package org.codelibs.elasticsearch.client.action;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpAnalyzeAction extends HttpAction {

    protected final AnalyzeAction action;

    public HttpAnalyzeAction(final HttpClient client, final AnalyzeAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final AnalyzeRequest request, final ActionListener<AnalyzeResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final AnalyzeResponse cancelTasksResponse = AnalyzeResponse.fromXContent(parser);
                listener.onResponse(cancelTasksResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final AnalyzeRequest request) {
        // RestAnalyzeAction
        final CurlRequest curlRequest =
                client.getCurlRequest(POST, "/_analyze", request.index() == null ? new String[0] : request.indices());
        return curlRequest;
    }
}
