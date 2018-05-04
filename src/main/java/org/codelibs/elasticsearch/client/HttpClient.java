package org.codelibs.elasticsearch.client;

import java.util.concurrent.ForkJoinPool;

import org.codelibs.elasticsearch.client.net.Curl;
import org.codelibs.elasticsearch.client.net.CurlRequest;
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
            // org.elasticsearch.action.search.SearchAction
            @SuppressWarnings("unchecked")
            final ActionListener<SearchResponse> actionListener = (ActionListener<SearchResponse>) listener;
            processSearchAction((SearchAction) action, (SearchRequest) request, actionListener);
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
            // org.elasticsearch.action.admin.indices.refresh.RefreshAction
            // org.elasticsearch.action.admin.indices.flush.FlushAction
            // org.elasticsearch.action.admin.indices.flush.SyncedFlushAction
            // org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction
            // org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction
            // org.elasticsearch.action.admin.indices.upgrade.post.UpgradeSettingsAction
            // org.elasticsearch.action.admin.indices.upgrade.post.UpgradeAction
            // org.elasticsearch.action.admin.indices.upgrade.get.UpgradeStatusAction
            // org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheAction
            // org.elasticsearch.action.admin.indices.open.OpenIndexAction
            // org.elasticsearch.action.admin.indices.close.CloseIndexAction
            // org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction
            // org.elasticsearch.action.admin.indices.exists.types.TypesExistsAction
            // org.elasticsearch.action.admin.indices.rollover.RolloverAction
            // org.elasticsearch.action.admin.indices.analyze.AnalyzeAction
            // org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction
            // org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction
            // org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateAction
            // org.elasticsearch.action.admin.indices.create.CreateIndexAction
            // org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction
            // org.elasticsearch.action.admin.indices.alias.exists.AliasesExistAction
            // org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction
            // org.elasticsearch.action.admin.indices.segments.IndicesSegmentsAction
            // org.elasticsearch.action.admin.indices.stats.IndicesStatsAction
            // org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction
            // org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction
            // org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction
            // org.elasticsearch.action.admin.indices.recovery.RecoveryAction
            // org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction
            // org.elasticsearch.action.admin.indices.get.GetIndexAction
            // org.elasticsearch.action.admin.indices.delete.DeleteIndexAction
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

    protected void processSearchAction(final SearchAction action, final SearchRequest request, final ActionListener<SearchResponse> listener) {
        getPostRequest(request.indices()).param("request_cache", request.requestCache() != null ? request.requestCache().toString() : null)
                .param("routing", request.routing()).param("preference", request.preference()).body(request.source().toString())
                .execute(response -> {
                    if (response.getHttpStatusCode() != 200) {
                        throw new ElasticsearchException("Content is not found: " + response.getHttpStatusCode());
                    }
                    try {
                        final XContent xContent = XContentFactory.xContent(XContentType.JSON);
                        final XContentParser parser = xContent.createParser(NamedXContentRegistry.EMPTY, response.getContentAsStream());
                        final SearchResponse searchResponse = SearchResponse.fromXContent(parser);
                        listener.onResponse(searchResponse);
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }, e -> listener.onFailure(e));
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
        // TODO threadPool
        return Curl.post(buf.toString()).header("Content-Type", "application/json").threadPool(ForkJoinPool.commonPool());
    }
}
