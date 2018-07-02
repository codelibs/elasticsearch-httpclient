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
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.function.Supplier;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

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
import org.elasticsearch.action.admin.indices.flush.FlushAction;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
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
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.ImmutableOpenMap;
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

    protected static final ParseField ACKNOWLEDGED_FIELD = new ParseField("acknowledged");

    protected static final ParseField ALIASES_FIELD = new ParseField("aliases");

    protected static final ParseField MAPPINGS_FIELD = new ParseField("mappings");

    protected static final ParseField SETTINGS_FIELD = new ParseField("settings");

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
        } else if (GetIndexAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.get.GetIndexAction
            @SuppressWarnings("unchecked")
            final ActionListener<GetIndexResponse> actionListener = (ActionListener<GetIndexResponse>) listener;
            processGetIndexAction((GetIndexAction) action, (GetIndexRequest) request, actionListener);
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
        } else if (GetMappingsAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction
            @SuppressWarnings("unchecked")
            final ActionListener<GetMappingsResponse> actionListener = (ActionListener<GetMappingsResponse>) listener;
            processGetMappingsAction((GetMappingsAction) action, (GetMappingsRequest) request, actionListener);
        } else if (FlushAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.flush.FlushAction
            @SuppressWarnings("unchecked")
            final ActionListener<FlushResponse> actionListener = (ActionListener<FlushResponse>) listener;
            processFlushAction((FlushAction) action, (FlushRequest) request, actionListener);
        } else if (ClearScrollAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.search.ClearScrollAction
            @SuppressWarnings("unchecked")
            final ActionListener<ClearScrollResponse> actionListener = (ActionListener<ClearScrollResponse>) listener;
            processClearScrollAction((ClearScrollAction) action, (ClearScrollRequest) request, actionListener);
        } else if (MultiSearchAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.search.MultiSearchAction
            @SuppressWarnings("unchecked")
            final ActionListener<MultiSearchResponse> actionListener = (ActionListener<MultiSearchResponse>) listener;
            processMultiSearchAction((MultiSearchAction) action, (MultiSearchRequest) request, actionListener);
        } else if (SearchScrollAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.search.MultiSearchAction
            @SuppressWarnings("unchecked")
            final ActionListener<SearchResponse> actionListener = (ActionListener<SearchResponse>) listener;
            processSearchScrollAction((SearchScrollAction) action, (SearchScrollRequest) request, actionListener);
        } else {
            // org.elasticsearch.action.ingest.DeletePipelineAction
            // org.elasticsearch.action.ingest.PutPipelineAction
            // org.elasticsearch.action.ingest.SimulatePipelineAction
            // org.elasticsearch.action.ingest.GetPipelineAction
            // org.elasticsearch.action.termvectors.MultiTermVectorsAction
            // org.elasticsearch.action.termvectors.TermVectorsAction
            // org.elasticsearch.action.admin.indices.shrink.ResizeAction
            // org.elasticsearch.action.admin.indices.shrink.ShrinkAction
            // org.elasticsearch.action.admin.indices.shards.IndicesShardStoresAction
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
            // org.elasticsearch.action.admin.indices.recovery.RecoveryAction
            // org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction
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

    protected void processClearScrollAction(final ClearScrollAction action, final ClearScrollRequest request,
            final ActionListener<ClearScrollResponse> listener) {
        String source = null;
        try {
            XContentBuilder builder =
                    XContentFactory.jsonBuilder().startObject().array("scroll_id", request.getScrollIds().toArray(new String[0]))
                            .endObject();
            source = builder.string();
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to parse a reqsuest.", e);
        }

        getCurlRequest(DELETE, "/_search/scroll").body(source).execute(response -> {
            if (response.getHttpStatusCode() != 200) {
                throw new ElasticsearchException("Content is not found: " + response.getHttpStatusCode());
            }
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final ClearScrollResponse clearScrollResponse = ClearScrollResponse.fromXContent(parser);
                listener.onResponse(clearScrollResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }

    protected void processMultiSearchAction(final MultiSearchAction action, final MultiSearchRequest request,
            final ActionListener<MultiSearchResponse> listener) {
        String source = null;
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            for (SearchRequest searchRequest : request.requests()) {
                builder.startObject().array("index", searchRequest.indices()).endObject().startObject()
                        .array("query", searchRequest.source().toString()).endObject();
            }
            source = builder.string();
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to parse a request.", e);
        }

        getCurlRequest(POST, "/_msearch").body(source).execute(response -> {
            if (response.getHttpStatusCode() != 200) {
                throw new ElasticsearchException("Content is not found: " + response.getHttpStatusCode());
            }

            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                MultiSearchResponse multiSearchResponse = MultiSearchResponse.fromXContext(parser);
                listener.onResponse(multiSearchResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }

    protected void processSearchScrollAction(final SearchScrollAction action, final SearchScrollRequest request,
            final ActionListener<SearchResponse> listener) {
        String source = null;
        try {
            source = request.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS).string();
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to parse a request.", e);
        }

        getCurlRequest(POST, "/_search/scroll").body(source).execute(response -> {
            if (response.getHttpStatusCode() != 200) {
                throw new ElasticsearchException("Content is not found: " + response.getHttpStatusCode());
            }
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final SearchResponse scrollResponse = SearchResponse.fromXContent(parser);
                listener.onResponse(scrollResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }

    protected void processBulkAction(final BulkAction action, final BulkRequest request, final ActionListener<BulkResponse> listener) {
        String source = null;

        // todo

        getCurlRequest(POST, "/_bulk").body(source).execute(response -> {
            if (response.getHttpStatusCode() != 200) {
                throw new ElasticsearchException("not found: " + response.getHttpStatusCode());
            }
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final BulkResponse bulkResponse = BulkResponse.fromXContent(parser);
                listener.onResponse(bulkResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
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

    protected void processGetIndexAction(final GetIndexAction action, final GetIndexRequest request,
            final ActionListener<GetIndexResponse> listener) {
        getCurlRequest(GET, "/", request.indices()).execute(response -> {
            if (response.getHttpStatusCode() != 200) {
                throw new ElasticsearchException("Indices are not found: " + response.getHttpStatusCode());
            }
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final GetIndexResponse getIndexResponse = getGetIndexResponse(parser, action::newResponse);
                listener.onResponse(getIndexResponse);
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

    protected void processSearchAction(final SearchAction action, final SearchRequest request, final ActionListener<SearchResponse> listener) {
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

    protected void processGetMappingsAction(final GetMappingsAction action, final GetMappingsRequest request,
            final ActionListener<GetMappingsResponse> listener) {
        getCurlRequest(GET, "/_mapping/" + String.join(",", request.types()), request.indices()).execute(response -> {
            if (response.getHttpStatusCode() != 200) {
                throw new ElasticsearchException("Indices are not found: " + response.getHttpStatusCode());
            }
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final GetMappingsResponse getMappingsResponse = getGetMappingsResponse(parser, action::newResponse);
                listener.onResponse(getMappingsResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }

    protected void processFlushAction(final FlushAction action, final FlushRequest request, final ActionListener<FlushResponse> listener) {
        getCurlRequest(POST, "/_flush", request.indices()).param("wait_if_ongoing", String.valueOf(request.waitIfOngoing()))
                .param("force", String.valueOf(request.force())).execute(response -> {
                    if (response.getHttpStatusCode() != 200) {
                        throw new ElasticsearchException("Indices are not found: " + response.getHttpStatusCode());
                    }
                    try (final InputStream in = response.getContentAsStream()) {
                        final XContentParser parser = createParser(in);
                        final FlushResponse flushResponse = getResponseFromXContent(parser, action::newResponse);
                        listener.onResponse(flushResponse);
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

    protected GetIndexResponse getGetIndexResponse(final XContentParser parser, final Supplier<GetIndexResponse> newResponse)
            throws IOException {
        List<String> indices = new ArrayList<>();
        ImmutableOpenMap.Builder<String, List<AliasMetaData>> aliasesMapBuilder = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> mappingsMapBuilder = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, Settings> settingsMapBuilder = ImmutableOpenMap.builder();

        String index = null;
        XContentParser.Token token = parser.nextToken();
        while ((token = parser.nextToken()) != Token.END_OBJECT) {
            if (token == Token.FIELD_NAME) {
                index = parser.currentName();
                indices.add(index);
            } else if (token == Token.START_OBJECT) {
                while (parser.nextToken() == Token.FIELD_NAME) {
                    String currentFieldName = parser.currentName();
                    if (ALIASES_FIELD.match(currentFieldName)) {
                        aliasesMapBuilder.put(index, getAliasesFromXContent(parser));
                    } else if (MAPPINGS_FIELD.match(currentFieldName)) {
                        mappingsMapBuilder.put(index, getMappingsFromXContent(parser));
                    } else if (SETTINGS_FIELD.match(currentFieldName)) {
                        settingsMapBuilder.put(index, getSettingsFromXContent(parser));
                    }
                }
            }
        }

        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = mappingsMapBuilder.build();
        ImmutableOpenMap<String, List<AliasMetaData>> aliases = aliasesMapBuilder.build();
        ImmutableOpenMap<String, Settings> settings = settingsMapBuilder.build();

        try (ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
            out.writeStringArray(indices.toArray(new String[indices.size()]));
            out.writeVInt(mappings.size());
            for (ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> indexEntry : mappings) {
                out.writeString(indexEntry.key);
                out.writeVInt(indexEntry.value.size());
                for (ObjectObjectCursor<String, MappingMetaData> mappingEntry : indexEntry.value) {
                    out.writeString(mappingEntry.key);
                    mappingEntry.value.writeTo(out);
                }
            }
            out.writeVInt(aliases.size());
            for (ObjectObjectCursor<String, List<AliasMetaData>> indexEntry : aliases) {
                out.writeString(indexEntry.key);
                out.writeVInt(indexEntry.value.size());
                for (AliasMetaData aliasEntry : indexEntry.value) {
                    aliasEntry.writeTo(out);
                }
            }
            out.writeVInt(settings.size());
            for (ObjectObjectCursor<String, Settings> indexEntry : settings) {
                out.writeString(indexEntry.key);
                Settings.writeSettingsToStream(indexEntry.value, out);
            }
            final GetIndexResponse response = newResponse.get();
            response.readFrom(out.toStreamInput());
            return response;
        }
    }

    protected GetMappingsResponse getGetMappingsResponse(final XContentParser parser, final Supplier<GetMappingsResponse> newResponse)
            throws IOException {
        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> indexMapBuilder = ImmutableOpenMap.builder();

        String index = null;
        Token token = parser.nextToken();
        if (token != null) {
            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                if (token == Token.FIELD_NAME) {
                    index = parser.currentName();
                } else if (token == Token.START_OBJECT) {
                    while (parser.nextToken() == Token.FIELD_NAME) {
                        if (MAPPINGS_FIELD.match(parser.currentName())) {
                            indexMapBuilder.put(index, getMappingsFromXContent(parser));
                            break;
                        } else {
                            parser.skipChildren();
                        }
                    }
                }
            }
        }
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = indexMapBuilder.build();

        try (ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
            out.writeVInt(mappings.size());
            for (ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> indexEntry : mappings) {
                out.writeString(indexEntry.key);
                out.writeVInt(indexEntry.value.size());
                for (ObjectObjectCursor<String, MappingMetaData> typeEntry : indexEntry.value) {
                    out.writeString(typeEntry.key);
                    typeEntry.value.writeTo(out);
                }
            }

            final GetMappingsResponse response = newResponse.get();
            response.readFrom(out.toStreamInput());
            return response;
        }
    }

    protected List<AliasMetaData> getAliasesFromXContent(final XContentParser parser) throws IOException {
        List<AliasMetaData> aliases = new ArrayList<>();
        Token token = parser.nextToken();
        if (token == null) {
            return aliases;
        }
        while ((token = parser.nextToken()) != Token.END_OBJECT) {
            if (token == Token.FIELD_NAME) {
                aliases.add(AliasMetaData.Builder.fromXContent(parser));
            }
        }
        return aliases;
    }

    protected ImmutableOpenMap<String, MappingMetaData> getMappingsFromXContent(final XContentParser parser) throws IOException {
        ImmutableOpenMap.Builder<String, MappingMetaData> mappingsBuilder = ImmutableOpenMap.builder();
        String type = null;
        Token token = parser.nextToken();
        if (token == null) {
            return mappingsBuilder.build();
        }
        while ((token = parser.nextToken()) != Token.END_OBJECT) {
            if (token == Token.FIELD_NAME) {
                type = parser.currentName();
            } else if (token == Token.START_OBJECT) {
                Map<String, Object> mapping = parser.mapOrdered();
                mappingsBuilder.put(type, new MappingMetaData(type, mapping));
            }
        }
        return mappingsBuilder.build();
    }

    protected Settings getSettingsFromXContent(final XContentParser parser) throws IOException {
        if (parser.nextToken() == null) {
            return Settings.EMPTY;
        }
        return Settings.fromXContent(parser);
    }

    protected <T extends AcknowledgedResponse> T getAcknowledgedResponse(final XContentParser parser, final Supplier<T> newResponse)
            throws IOException {
        boolean acknowledged = false;

        String currentFieldName = null;
        Token token = parser.nextToken();
        if (token != null) {
            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                if (token == Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == Token.VALUE_BOOLEAN) {
                    if (ACKNOWLEDGED_FIELD.match(currentFieldName)) {
                        acknowledged = parser.booleanValue();
                    }
                }
            }
        }

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
