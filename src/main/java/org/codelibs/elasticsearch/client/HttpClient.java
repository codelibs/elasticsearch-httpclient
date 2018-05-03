package org.codelibs.elasticsearch.client;

import org.codelibs.elasticsearch.runner.net.Curl;
import org.codelibs.elasticsearch.runner.net.CurlRequest;
import org.codelibs.elasticsearch.runner.net.CurlResponse;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.threadpool.ThreadPool;

public class HttpClient extends AbstractClient {

    private String[] hosts;

    public HttpClient(final Settings settings, final ThreadPool threadPool) {
        super(settings, threadPool);
        hosts = settings.getAsList("http.hosts").stream().map(s -> {
            if (!s.startsWith("http:") && !s.startsWith("https:")) {
                return "http://" + s;
            }
            return s;
        }).toArray(n -> new String[n]);
        if (hosts.length == 0) {
            throw new ElasticsearchException("http.hosts is empty.");
        }
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(
            final Action<Request, Response, RequestBuilder> action, final Request request, final ActionListener<Response> listener) {
        if (SearchAction.INSTANCE.equals(action)) {
            @SuppressWarnings("unchecked")
            final ActionListener<SearchResponse> actionListener = (ActionListener<SearchResponse>) listener;
            processSearchAction((SearchAction) action, (SearchRequest) request, actionListener);
        } else {
            throw new UnsupportedOperationException("Action: " + action.name());
        }
    }

    protected void processSearchAction(final SearchAction action, final SearchRequest request, final ActionListener<SearchResponse> listener) {
        try (CurlResponse response = getPostRequest(request.indices()).body(request.source().toString()).execute()) {
            if (response.getHttpStatusCode() != 200) {
                throw new ElasticsearchException("Content is not found: " + response.getHttpStatusCode());
            }
            final XContent xContent = XContentFactory.xContent(XContentType.JSON);
            final XContentParser parser = xContent.createParser(NamedXContentRegistry.EMPTY, response.getContentAsStream());
            final SearchResponse searchResponse = SearchResponse.fromXContent(parser);
            listener.onResponse(searchResponse);
        } catch (final Exception e) {
            listener.onFailure(e);
        }
    }

    protected String getHost() {
        return hosts[0];
    }

    protected CurlRequest getPostRequest(final String[] indices) {
        final StringBuilder buf = new StringBuilder(100);
        buf.append(getHost());
        if (indices.length > 0) {
            buf.append('/').append(String.join(",", indices));
        }
        buf.append("/_search");
        // TODO other request headers
        return Curl.post(buf.toString()).header("Content-Type", "application/json");
    }
}
