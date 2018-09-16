package org.codelibs.elasticsearch.client.action;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ingest.SimulatePipelineAction;
import org.elasticsearch.action.ingest.SimulatePipelineRequest;
import org.elasticsearch.action.ingest.SimulatePipelineResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpSimulatePipelineAction extends HttpAction {

    protected final SimulatePipelineAction action;

    public HttpSimulatePipelineAction(final HttpClient client, final SimulatePipelineAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final SimulatePipelineRequest request, final ActionListener<SimulatePipelineResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final SimulatePipelineResponse cancelTasksResponse = SimulatePipelineResponse.fromXContent(parser);
                listener.onResponse(cancelTasksResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final SimulatePipelineRequest request) {
        // RestSimulatePipelineAction
        final String path = request.getId() != null ? "/_ingest/pipeline/" + request.getId() + "/_simulate" : "/_ingest/pipeline/_simulate";
        final CurlRequest curlRequest = client.getCurlRequest(POST, path);
        curlRequest.param("verbose", String.valueOf(request.isVerbose()));
        return curlRequest;
    }
}
