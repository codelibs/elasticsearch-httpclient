package org.codelibs.elasticsearch.client;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.rest.action.RestActions.FAILED_FIELD;
import static org.elasticsearch.rest.action.RestActions.FAILURES_FIELD;
import static org.elasticsearch.rest.action.RestActions.SUCCESSFUL_FIELD;
import static org.elasticsearch.rest.action.RestActions.TOTAL_FIELD;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codelibs.elasticsearch.client.io.stream.ByteArrayStreamOutput;
import org.codelibs.elasticsearch.client.net.Curl;
import org.codelibs.elasticsearch.client.net.CurlRequest;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.threadpool.ThreadPool;

public class HttpClient extends AbstractClient {

    protected static final ParseField SHARD_FIELD = new ParseField("shard");

    protected static final ParseField INDEX_FIELD = new ParseField("index");

    protected static final ParseField STATUS_FIELD = new ParseField("status");

    protected static final ParseField REASON_FIELD = new ParseField("reason");

    protected static Function<String, CurlRequest> GET = s -> Curl.get(s);

    protected static Function<String, CurlRequest> POST = s -> Curl.post(s);

    protected static Function<String, CurlRequest> PUT = s -> Curl.put(s);

    protected static Function<String, CurlRequest> DELETE = s -> Curl.delete(s);

    protected static Function<String, CurlRequest> HEAD = s -> Curl.head(s);

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
            // org.elasticsearch.action.search.SearchAction
            @SuppressWarnings("unchecked")
            final ActionListener<SearchResponse> actionListener = (ActionListener<SearchResponse>) listener;
            processSearchAction((SearchAction) action, (SearchRequest) request, actionListener);
        } else if (RefreshAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.refresh.RefreshAction
            @SuppressWarnings("unchecked")
            final ActionListener<RefreshResponse> actionListener = (ActionListener<RefreshResponse>) listener;
            processRefreshAction((RefreshAction) action, (RefreshRequest) request, actionListener);
        } else if (CreateIndexAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.create.CreateIndexAction
            @SuppressWarnings("unchecked")
            final ActionListener<CreateIndexResponse> actionListener = (ActionListener<CreateIndexResponse>) listener;
            processCreateIndexAction((CreateIndexAction) action, (CreateIndexRequest) request, actionListener);
        } else if (DeleteIndexAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.delete.DeleteIndexAction
            @SuppressWarnings("unchecked")
            final ActionListener<DeleteIndexResponse> actionListener = (ActionListener<DeleteIndexResponse>) listener;
            processDeleteIndexAction((DeleteIndexAction) action, (DeleteIndexRequest) request, actionListener);
        } else if (OpenIndexAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.open.OpenIndexAction
            @SuppressWarnings("unchecked")
            final ActionListener<OpenIndexResponse> actionListener = (ActionListener<OpenIndexResponse>) listener;
            processOpenIndexAction((OpenIndexAction) action, (OpenIndexRequest) request, actionListener);
        } else if (CloseIndexAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.close.CloseIndexAction
            @SuppressWarnings("unchecked")
            final ActionListener<CloseIndexResponse> actionListener = (ActionListener<CloseIndexResponse>) listener;
            processCloseIndexAction((CloseIndexAction) action, (CloseIndexRequest) request, actionListener);
        } else if (IndicesExistsAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction
            @SuppressWarnings("unchecked")
            final ActionListener<IndicesExistsResponse> actionListener = (ActionListener<IndicesExistsResponse>) listener;
            processIndicesExistsAction((IndicesExistsAction) action, (IndicesExistsRequest) request, actionListener);
        } else if (IndicesAliasesAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction
            @SuppressWarnings("unchecked")
            final ActionListener<IndicesAliasesResponse> actionListener = (ActionListener<IndicesAliasesResponse>) listener;
            processIndicesAliasesAction((IndicesAliasesAction) action, (IndicesAliasesRequest) request, actionListener);
        } else if (PutMappingAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction
            @SuppressWarnings("unchecked")
            final ActionListener<PutMappingResponse> actionListener = (ActionListener<PutMappingResponse>) listener;
            processPutMappingAction((PutMappingAction) action, (PutMappingRequest) request, actionListener);
        } else {
            // org.elasticsearch.action.search.ClearScrollAction
            // org.elasticsearch.action.search.MultiSearchAction
            // org.elasticsearch.action.search.SearchScrollAction
            // org.elasticsearch.action.ingest.DeletePipelineAction
            // org.elasticsearch.action.ingest.PutPipelineAction
            // org.elasticsearch.action.ingest.SimulatePipelineAction
            // org.elasticsearch.action.ingest.GetPipelineAction
            // org.elasticsearch.action.termvectors.MultiTermVectorsAction
            // org.elasticsearch.action.termvectors.TermVectorsAction
            // org.elasticsearch.action.admin.indices.shrink.ResizeAction
            // org.elasticsearch.action.admin.indices.shrink.ShrinkAction
            // org.elasticsearch.action.admin.indices.shards.IndicesShardStoresAction
            // org.elasticsearch.action.admin.indices.flush.FlushAction
            // org.elasticsearch.action.admin.indices.flush.SyncedFlushAction
            // org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction
            // org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction
            // org.elasticsearch.action.admin.indices.upgrade.post.UpgradeSettingsAction
            // org.elasticsearch.action.admin.indices.upgrade.post.UpgradeAction
            // org.elasticsearch.action.admin.indices.upgrade.get.UpgradeStatusAction
            // org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheAction
            // org.elasticsearch.action.admin.indices.exists.types.TypesExistsAction
            // org.elasticsearch.action.admin.indices.rollover.RolloverAction
            // org.elasticsearch.action.admin.indices.analyze.AnalyzeAction
            // org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction
            // org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction
            // org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateAction
            // org.elasticsearch.action.admin.indices.alias.exists.AliasesExistAction
            // org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction
            // org.elasticsearch.action.admin.indices.segments.IndicesSegmentsAction
            // org.elasticsearch.action.admin.indices.stats.IndicesStatsAction
            // org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction
            // org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction
            // org.elasticsearch.action.admin.indices.recovery.RecoveryAction
            // org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction
            // org.elasticsearch.action.admin.indices.get.GetIndexAction
            // org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction
            // org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsAction
            // org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryAction
            // org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryAction
            // org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesAction
            // org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryAction
            // org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksAction
            // org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsAction
            // org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksAction
            // org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction
            // org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskAction
            // org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction
            // org.elasticsearch.action.admin.cluster.node.usage.NodesUsageAction
            // org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction
            // org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction
            // org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusAction
            // org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction
            // org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsAction
            // org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotAction
            // org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction
            // org.elasticsearch.action.admin.cluster.remote.RemoteInfoAction
            // org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptAction
            // org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptAction
            // org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptAction
            // org.elasticsearch.action.admin.cluster.state.ClusterStateAction
            // org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainAction
            // org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteAction
            // org.elasticsearch.action.admin.cluster.stats.ClusterStatsAction
            // org.elasticsearch.action.admin.cluster.health.ClusterHealthAction
            // org.elasticsearch.action.index.IndexAction
            // org.elasticsearch.action.fieldcaps.FieldCapabilitiesAction
            // org.elasticsearch.action.update.UpdateAction
            // org.elasticsearch.action.main.MainAction
            // org.elasticsearch.action.get.GetAction
            // org.elasticsearch.action.get.MultiGetAction
            // org.elasticsearch.action.explain.ExplainAction
            // org.elasticsearch.action.delete.DeleteAction
            // org.elasticsearch.action.bulk.BulkAction
            throw new UnsupportedOperationException("Action: " + action.name());
        }
    }

    protected void processCreateIndexAction(final CreateIndexAction action, final CreateIndexRequest request,
            final ActionListener<CreateIndexResponse> listener) {
        String source = null;
        try {
            source = request.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS).string();
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to parse a request.", e);
        }
        getCurlRequest(PUT, "/", request.index()).body(source).execute(response -> {
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final CreateIndexResponse refreshResponse = CreateIndexResponse.fromXContent(parser);
                listener.onResponse(refreshResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }

    protected void processDeleteIndexAction(final DeleteIndexAction action, final DeleteIndexRequest request,
            final ActionListener<DeleteIndexResponse> listener) {
        getCurlRequest(DELETE, "/", request.indices()).execute(response -> {
            if (response.getHttpStatusCode() != 200) {
                throw new ElasticsearchException("Indices are not found: " + response.getHttpStatusCode());
            }
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final DeleteIndexResponse deleteIndexResponse = DeleteIndexResponse.fromXContent(parser);
                listener.onResponse(deleteIndexResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }

    protected void processOpenIndexAction(final OpenIndexAction action, final OpenIndexRequest request,
            final ActionListener<OpenIndexResponse> listener) {
        getCurlRequest(POST, "/_open", request.indices()).execute(response -> {
            if (response.getHttpStatusCode() != 200) {
                throw new ElasticsearchException("Indices are not found: " + response.getHttpStatusCode());
            }
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final OpenIndexResponse openIndexResponse = OpenIndexResponse.fromXContent(parser);
                listener.onResponse(openIndexResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }

    protected void processCloseIndexAction(final CloseIndexAction action, final CloseIndexRequest request,
            final ActionListener<CloseIndexResponse> listener) {
        getCurlRequest(POST, "/_close", request.indices()).execute(response -> {
            if (response.getHttpStatusCode() != 200) {
                throw new ElasticsearchException("Indices are not found: " + response.getHttpStatusCode());
            }
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final CloseIndexResponse closeIndexResponse = CloseIndexResponse.fromXContent(parser);
                listener.onResponse(closeIndexResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }

    protected void processRefreshAction(final RefreshAction action, final RefreshRequest request,
            final ActionListener<RefreshResponse> listener) {
        getCurlRequest(POST, "/_refresh", request.indices()).execute(response -> {
            if (response.getHttpStatusCode() != 200) {
                throw new ElasticsearchException("Indices are not found: " + response.getHttpStatusCode());
            }
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final RefreshResponse refreshResponse = getResponseFromXContent(parser, action::newResponse);
                listener.onResponse(refreshResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }

    protected void processSearchAction(final SearchAction action, final SearchRequest request,
            final ActionListener<SearchResponse> listener) {
        getCurlRequest(POST, "/_search", request.indices())
                .param("request_cache", request.requestCache() != null ? request.requestCache().toString() : null)
                .param("routing", request.routing()).param("preference", request.preference()).body(request.source().toString())
                .execute(response -> {
                    if (response.getHttpStatusCode() != 200) {
                        throw new ElasticsearchException("Content is not found: " + response.getHttpStatusCode());
                    }
                    try (final InputStream in = response.getContentAsStream()) {
                        final XContentParser parser = createParser(in);
                        final SearchResponse searchResponse = SearchResponse.fromXContent(parser);
                        listener.onResponse(searchResponse);
                    } catch (final Exception e) {
                        listener.onFailure(e);
                    }
                }, listener::onFailure);
    }

    protected void processIndicesExistsAction(final IndicesExistsAction action, final IndicesExistsRequest request,
            final ActionListener<IndicesExistsResponse> listener) {
        getCurlRequest(HEAD, "", request.indices()).execute(response -> {
            boolean exists = false;
            switch (response.getHttpStatusCode()) {
            case 200:
                exists = true;
                break;
            case 404:
                exists = false;
                break;
            default:
                throw new ElasticsearchException("Unexpected status: " + response.getHttpStatusCode());
            }
            try {
                final IndicesExistsResponse indicesExistsResponse = new IndicesExistsResponse(exists);
                listener.onResponse(indicesExistsResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);

    }

    protected void processIndicesAliasesAction(final IndicesAliasesAction action, final IndicesAliasesRequest request,
            final ActionListener<IndicesAliasesResponse> listener) {
        String source = null;
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startArray("actions");
            for (AliasActions aliasAction : request.getAliasActions()) {
                builder.startObject().startObject(aliasAction.actionType().toString().toLowerCase());
                builder.array("indices", aliasAction.indices());
                builder.array("aliases", aliasAction.aliases());
                if (aliasAction.filter() != null)
                    builder.field("filter", aliasAction.filter());
                if (aliasAction.indexRouting() != null)
                    builder.field("index_routing", aliasAction.indexRouting());
                if (aliasAction.searchRouting() != null)
                    builder.field("search_routing", aliasAction.searchRouting());
                builder.endObject().endObject();
            }
            builder.endArray().endObject();
            source = builder.string();
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to parse a request.", e);
        }
        getCurlRequest(POST, "/_aliases").body(source).execute(response -> {
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final IndicesAliasesResponse indicesAliasesResponse = getAcknowledgedResponse(parser, action::newResponse);
                listener.onResponse(indicesAliasesResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);

    }

    protected void processPutMappingAction(final PutMappingAction action, final PutMappingRequest request,
            final ActionListener<PutMappingResponse> listener) {
        getCurlRequest(PUT, "/_mapping/" + request.type(), request.indices()).body(request.source()).execute(response -> {
            if (response.getHttpStatusCode() != 200) {
                throw new ElasticsearchException("Indices are not found: " + response.getHttpStatusCode());
            }
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final PutMappingResponse putMappingResponse = getAcknowledgedResponse(parser, action::newResponse);
                listener.onResponse(putMappingResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }

    protected <T extends BroadcastResponse> T getResponseFromXContent(final XContentParser parser, final Supplier<T> newResponse)
            throws IOException {
        ensureExpectedToken(Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
        parser.nextToken();
        ensureExpectedToken(Token.FIELD_NAME, parser.currentToken(), parser::getTokenLocation);
        String currentFieldName = parser.currentName(); // _SHARDS_FIELD
        int totalShards = 0;
        int successfulShards = 0;
        int failedShards = 0;
        final List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>();
        for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
            if (token == Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == Token.START_ARRAY) {
                if (FAILURES_FIELD.match(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token != XContentParser.Token.START_OBJECT) {
                            throw new ElasticsearchException("failures array element should include an object");
                        }
                        shardFailures.add(getFailureFromXContent(parser));
                    }
                } else {
                    parser.skipChildren();
                }
            } else if (token.isValue()) {
                if (TOTAL_FIELD.match(currentFieldName)) {
                    totalShards = parser.intValue();
                } else if (SUCCESSFUL_FIELD.match(currentFieldName)) {
                    successfulShards = parser.intValue();
                } else if (FAILED_FIELD.match(currentFieldName)) {
                    failedShards = parser.intValue();
                } else {
                    parser.skipChildren();
                }
            }
        }

        // BroadcastResponse
        try (ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
            out.writeVInt(totalShards);
            out.writeVInt(successfulShards);
            out.writeVInt(failedShards);
            out.writeVInt(shardFailures.size());
            for (final ShardOperationFailedException exp : shardFailures) {
                exp.writeTo(out);
            }

            final T response = newResponse.get();
            response.readFrom(out.toStreamInput());
            return response;
        }
    }

    protected <T extends AcknowledgedResponse> T getAcknowledgedResponse(final XContentParser parser, final Supplier<T> newResponse)
            throws IOException {
        ensureExpectedToken(Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
        ensureExpectedToken(Token.FIELD_NAME, parser.nextToken(), parser::getTokenLocation);
        ensureExpectedToken(Token.VALUE_BOOLEAN, parser.nextToken(), parser::getTokenLocation);
        boolean acknowledged = parser.booleanValue();
        ensureExpectedToken(Token.END_OBJECT, parser.nextToken(), parser::getTokenLocation);

        try (ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
            out.writeBoolean(acknowledged);
            final T response = newResponse.get();
            response.readFrom(out.toStreamInput());
            return response;
        }
    }

    protected DefaultShardOperationFailedException getFailureFromXContent(final XContentParser parser) throws IOException {
        String index = null;
        ElasticsearchException reason = null;
        int shardId = 0;
        String currentFieldName = "";
        for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
            if (token == Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (SHARD_FIELD.match(currentFieldName)) {
                    shardId = parser.intValue();
                } else if (INDEX_FIELD.match(currentFieldName)) {
                    index = parser.text();
                } else if (REASON_FIELD.match(currentFieldName)) {
                    reason = ElasticsearchException.fromXContent(parser);
                } else {
                    parser.skipChildren();
                }
            }
        }
        return new DefaultShardOperationFailedException(index, shardId, reason);
    }

    protected XContentParser createParser(final InputStream in) throws IOException {
        final XContent xContent = XContentFactory.xContent(XContentType.JSON);
        return xContent.createParser(NamedXContentRegistry.EMPTY, in);
    }

    protected String getHost() {
        return hosts[0];
    }

    protected CurlRequest getCurlRequest(final Function<String, CurlRequest> method, final String path, final String... indices) {
        final StringBuilder buf = new StringBuilder(100);
        buf.append(getHost());
        if (indices.length > 0) {
            buf.append('/').append(String.join(",", indices));
        }
        buf.append(path);
        // TODO other request headers
        // TODO threadPool
        return method.apply(buf.toString()).header("Content-Type", "application/json").threadPool(ForkJoinPool.commonPool());
    }
}
