/*
 * Copyright 2012-2018 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.elasticsearch.client;

import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.logging.Logger;

import org.codelibs.curl.Curl;
import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.action.*;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksAction;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksRequest;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistAction;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheAction;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
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
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsAction;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.flush.FlushAction;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushAction;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushRequest;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushResponse;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
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
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeResponse;
import org.elasticsearch.action.admin.indices.shrink.ShrinkAction;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.explain.ExplainAction;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesAction;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.ingest.DeletePipelineAction;
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.GetPipelineAction;
import org.elasticsearch.action.ingest.GetPipelineRequest;
import org.elasticsearch.action.ingest.GetPipelineResponse;
import org.elasticsearch.action.ingest.PutPipelineAction;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.ingest.SimulatePipelineAction;
import org.elasticsearch.action.ingest.SimulatePipelineRequest;
import org.elasticsearch.action.ingest.SimulatePipelineResponse;
import org.elasticsearch.action.ingest.WritePipelineResponse;
import org.elasticsearch.action.main.MainAction;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

public class HttpClient extends AbstractClient {

    protected static final Logger logger = Logger.getLogger(HttpClient.class.getName());

    protected static final Function<String, CurlRequest> GET = Curl::get;

    protected static final Function<String, CurlRequest> POST = Curl::post;

    protected static final Function<String, CurlRequest> PUT = Curl::put;

    protected static final Function<String, CurlRequest> DELETE = Curl::delete;

    protected static final Function<String, CurlRequest> HEAD = Curl::head;

    private String[] hosts;

    public enum ContentType {
        JSON("application/json"), X_NDJSON("application/x-ndjson");

        private final String value;

        private ContentType(final String value) {
            this.value = value;
        }

        public String getString() {
            return this.value;
        }
    }

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
        // TODO thread pool management
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(
            final Action<Request, Response, RequestBuilder> action, final Request request, final ActionListener<Response> listener) {
        if (SearchAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.search.SearchAction
            @SuppressWarnings("unchecked")
            final ActionListener<SearchResponse> actionListener = (ActionListener<SearchResponse>) listener;
            new HttpSearchAction(this, (SearchAction) action).execute((SearchRequest) request, actionListener);
        } else if (RefreshAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.refresh.RefreshAction
            @SuppressWarnings("unchecked")
            final ActionListener<RefreshResponse> actionListener = (ActionListener<RefreshResponse>) listener;
            new HttpRefreshAction(this, (RefreshAction) action).execute((RefreshRequest) request, actionListener);
        } else if (CreateIndexAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.create.CreateIndexAction
            @SuppressWarnings("unchecked")
            final ActionListener<CreateIndexResponse> actionListener = (ActionListener<CreateIndexResponse>) listener;
            new HttpCreateIndexAction(this, (CreateIndexAction) action).execute((CreateIndexRequest) request, actionListener);
        } else if (DeleteIndexAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.delete.DeleteIndexAction
            @SuppressWarnings("unchecked")
            final ActionListener<DeleteIndexResponse> actionListener = (ActionListener<DeleteIndexResponse>) listener;
            new HttpDeleteIndexAction(this, (DeleteIndexAction) action).execute((DeleteIndexRequest) request, actionListener);
        } else if (GetIndexAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.get.GetIndexAction
            @SuppressWarnings("unchecked")
            final ActionListener<GetIndexResponse> actionListener = (ActionListener<GetIndexResponse>) listener;
            new HttpGetIndexAction(this, (GetIndexAction) action).execute((GetIndexRequest) request, actionListener);
        } else if (OpenIndexAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.open.OpenIndexAction
            @SuppressWarnings("unchecked")
            final ActionListener<OpenIndexResponse> actionListener = (ActionListener<OpenIndexResponse>) listener;
            new HttpOpenIndexAction(this, (OpenIndexAction) action).execute((OpenIndexRequest) request, actionListener);
        } else if (CloseIndexAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.close.CloseIndexAction
            @SuppressWarnings("unchecked")
            final ActionListener<CloseIndexResponse> actionListener = (ActionListener<CloseIndexResponse>) listener;
            new HttpCloseIndexAction(this, (CloseIndexAction) action).execute((CloseIndexRequest) request, actionListener);
        } else if (IndicesExistsAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction
            @SuppressWarnings("unchecked")
            final ActionListener<IndicesExistsResponse> actionListener = (ActionListener<IndicesExistsResponse>) listener;
            new HttpIndicesExistsAction(this, (IndicesExistsAction) action).execute((IndicesExistsRequest) request, actionListener);
        } else if (IndicesAliasesAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction
            @SuppressWarnings("unchecked")
            final ActionListener<IndicesAliasesResponse> actionListener = (ActionListener<IndicesAliasesResponse>) listener;
            new HttpIndicesAliasesAction(this, (IndicesAliasesAction) action).execute((IndicesAliasesRequest) request, actionListener);
        } else if (PutMappingAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction
            @SuppressWarnings("unchecked")
            final ActionListener<PutMappingResponse> actionListener = (ActionListener<PutMappingResponse>) listener;
            new HttpPutMappingAction(this, (PutMappingAction) action).execute((PutMappingRequest) request, actionListener);
        } else if (GetMappingsAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction
            @SuppressWarnings("unchecked")
            final ActionListener<GetMappingsResponse> actionListener = (ActionListener<GetMappingsResponse>) listener;
            new HttpGetMappingsAction(this, (GetMappingsAction) action).execute((GetMappingsRequest) request, actionListener);
        } else if (GetFieldMappingsAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction
            @SuppressWarnings("unchecked")
            final ActionListener<GetFieldMappingsResponse> actionListener = (ActionListener<GetFieldMappingsResponse>) listener;
            new HttpGetFieldMappingsAction(this, (GetFieldMappingsAction) action)
                    .execute((GetFieldMappingsRequest) request, actionListener);
        } else if (FlushAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.flush.FlushAction
            @SuppressWarnings("unchecked")
            final ActionListener<FlushResponse> actionListener = (ActionListener<FlushResponse>) listener;
            new HttpFlushAction(this, (FlushAction) action).execute((FlushRequest) request, actionListener);
        } else if (ClearScrollAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.search.ClearScrollAction
            @SuppressWarnings("unchecked")
            final ActionListener<ClearScrollResponse> actionListener = (ActionListener<ClearScrollResponse>) listener;
            new HttpClearScrollAction(this, (ClearScrollAction) action).execute((ClearScrollRequest) request, actionListener);
        } else if (MultiSearchAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.search.MultiSearchAction
            @SuppressWarnings("unchecked")
            final ActionListener<MultiSearchResponse> actionListener = (ActionListener<MultiSearchResponse>) listener;
            new HttpMultiSearchAction(this, (MultiSearchAction) action).execute((MultiSearchRequest) request, actionListener);
        } else if (SearchScrollAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.search.MultiSearchAction
            @SuppressWarnings("unchecked")
            final ActionListener<SearchResponse> actionListener = (ActionListener<SearchResponse>) listener;
            new HttpSearchScrollAction(this, (SearchScrollAction) action).execute((SearchScrollRequest) request, actionListener);
        } else if (IndexAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.index.IndexAction
            @SuppressWarnings("unchecked")
            final ActionListener<IndexResponse> actionListener = (ActionListener<IndexResponse>) listener;
            new HttpIndexAction(this, (IndexAction) action).execute((IndexRequest) request, actionListener);
        } else if (FieldCapabilitiesAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.fieldcaps.FieldCapabilitiesAction)
            @SuppressWarnings("unchecked")
            final ActionListener<FieldCapabilitiesResponse> actionListener = (ActionListener<FieldCapabilitiesResponse>) listener;
            new HttpFieldCapabilitiesAction(this, (FieldCapabilitiesAction) action).execute((FieldCapabilitiesRequest) request,
                    actionListener);
        } else if (GetAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.get.GetAction
            @SuppressWarnings("unchecked")
            final ActionListener<GetResponse> actionListener = (ActionListener<GetResponse>) listener;
            new HttpGetAction(this, (GetAction) action).execute((GetRequest) request, actionListener);
        } else if (MultiGetAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.get.MultiGetAction
            @SuppressWarnings("unchecked")
            final ActionListener<MultiGetResponse> actionListener = (ActionListener<MultiGetResponse>) listener;
            new HttpMultiGetAction(this, (MultiGetAction) action).execute((MultiGetRequest) request, actionListener);
        } else if (UpdateAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.update.UpdateAction
            @SuppressWarnings("unchecked")
            final ActionListener<UpdateResponse> actionListener = (ActionListener<UpdateResponse>) listener;
            new HttpUpdateAction(this, (UpdateAction) action).execute((UpdateRequest) request, actionListener);
        } else if (BulkAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.bulk.BulkAction
            @SuppressWarnings("unchecked")
            final ActionListener<BulkResponse> actionListener = (ActionListener<BulkResponse>) listener;
            new HttpBulkAction(this, (BulkAction) action).execute((BulkRequest) request, actionListener);
        } else if (DeleteAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.delete.DeleteAction
            @SuppressWarnings("unchecked")
            final ActionListener<DeleteResponse> actionListener = (ActionListener<DeleteResponse>) listener;
            new HttpDeleteAction(this, (DeleteAction) action).execute((DeleteRequest) request, actionListener);
        } else if (ExplainAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.explain.ExplainAction
            @SuppressWarnings("unchecked")
            final ActionListener<ExplainResponse> actionListener = (ActionListener<ExplainResponse>) listener;
            new HttpExplainAction(this, (ExplainAction) action).execute((ExplainRequest) request, actionListener);
        } else if (UpdateSettingsAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction
            @SuppressWarnings("unchecked")
            final ActionListener<UpdateSettingsResponse> actionListener = (ActionListener<UpdateSettingsResponse>) listener;
            new HttpUpdateSettingsAction(this, (UpdateSettingsAction) action).execute((UpdateSettingsRequest) request, actionListener);
        } else if (GetSettingsAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction
            @SuppressWarnings("unchecked")
            final ActionListener<GetSettingsResponse> actionListener = (ActionListener<GetSettingsResponse>) listener;
            new HttpGetSettingsAction(this, (GetSettingsAction) action).execute((GetSettingsRequest) request, actionListener);
        } else if (ForceMergeAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction
            @SuppressWarnings("unchecked")
            final ActionListener<ForceMergeResponse> actionListener = (ActionListener<ForceMergeResponse>) listener;
            new HttpForceMergeAction(this, (ForceMergeAction) action).execute((ForceMergeRequest) request, actionListener);
        } else if (MainAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.main.MainAction
            @SuppressWarnings("unchecked")
            final ActionListener<MainResponse> actionListener = (ActionListener<MainResponse>) listener;
            new HttpMainAction(this, (MainAction) action).execute((MainRequest) request, actionListener);
        } else if (ClusterUpdateSettingsAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction
            @SuppressWarnings("unchecked")
            final ActionListener<ClusterUpdateSettingsResponse> actionListener = (ActionListener<ClusterUpdateSettingsResponse>) listener;
            new HttpClusterUpdateSettingsAction(this, (ClusterUpdateSettingsAction) action).execute((ClusterUpdateSettingsRequest) request,
                    actionListener);
        } else if (ClusterHealthAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.cluster.health.ClusterHealthAction
            @SuppressWarnings("unchecked")
            final ActionListener<ClusterHealthResponse> actionListener = (ActionListener<ClusterHealthResponse>) listener;
            new HttpClusterHealthAction(this, (ClusterHealthAction) action).execute((ClusterHealthRequest) request, actionListener);
        } else if (AliasesExistAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.alias.exists.AliasesExistAction
            @SuppressWarnings("unchecked")
            final ActionListener<AliasesExistResponse> actionListener = (ActionListener<AliasesExistResponse>) listener;
            new HttpAliasesExistAction(this, (AliasesExistAction) action).execute((GetAliasesRequest) request, actionListener);
        } else if (ValidateQueryAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction
            @SuppressWarnings("unchecked")
            final ActionListener<ValidateQueryResponse> actionListener = (ActionListener<ValidateQueryResponse>) listener;
            new HttpValidateQueryAction(this, (ValidateQueryAction) action).execute((ValidateQueryRequest) request, actionListener);
        } else if (PendingClusterTasksAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksAction
            @SuppressWarnings("unchecked")
            final ActionListener<PendingClusterTasksResponse> actionListener = (ActionListener<PendingClusterTasksResponse>) listener;
            new HttpPendingClusterTasksAction(this, (PendingClusterTasksAction) action).execute((PendingClusterTasksRequest) request,
                    actionListener);
        } else if (GetAliasesAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction
            @SuppressWarnings("unchecked")
            final ActionListener<GetAliasesResponse> actionListener = (ActionListener<GetAliasesResponse>) listener;
            new HttpGetAliasesAction(this, (GetAliasesAction) action).execute((GetAliasesRequest) request, actionListener);
        } else if (SyncedFlushAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.flush.SyncedFlushAction
            @SuppressWarnings("unchecked")
            final ActionListener<SyncedFlushResponse> actionListener = (ActionListener<SyncedFlushResponse>) listener;
            new HttpSyncedFlushAction(this, (SyncedFlushAction) action).execute((SyncedFlushRequest) request, actionListener);
        } else if (ShrinkAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.shrink.ShrinkAction
            @SuppressWarnings("unchecked")
            final ActionListener<ResizeResponse> actionListener = (ActionListener<ResizeResponse>) listener;
            new HttpShrinkAction(this, (ShrinkAction) action).execute((ResizeRequest) request, actionListener);
        } else if (TypesExistsAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.exists.types.TypesExistsAction
            @SuppressWarnings("unchecked")
            final ActionListener<TypesExistsResponse> actionListener = (ActionListener<TypesExistsResponse>) listener;
            new HttpTypesExistsAction(this, (TypesExistsAction) action).execute((TypesExistsRequest) request, actionListener);
        } else if (RolloverAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.rollover.RolloverAction
            @SuppressWarnings("unchecked")
            final ActionListener<RolloverResponse> actionListener = (ActionListener<RolloverResponse>) listener;
            new HttpRolloverAction(this, (RolloverAction) action).execute((RolloverRequest) request, actionListener);
        } else if (ClearIndicesCacheAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheAction
            @SuppressWarnings("unchecked")
            final ActionListener<ClearIndicesCacheResponse> actionListener = (ActionListener<ClearIndicesCacheResponse>) listener;
            new HttpClearIndicesCacheAction(this, (ClearIndicesCacheAction) action).execute((ClearIndicesCacheRequest) request,
                    actionListener);
        } else if (DeletePipelineAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.ingest.DeletePipelineAction
            @SuppressWarnings("unchecked")
            final ActionListener<WritePipelineResponse> actionListener = (ActionListener<WritePipelineResponse>) listener;
            new HttpDeletePipelineAction(this, (DeletePipelineAction) action).execute((DeletePipelineRequest) request, actionListener);
        } else if (PutPipelineAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.ingest.PutPipelineAction
            @SuppressWarnings("unchecked")
            final ActionListener<WritePipelineResponse> actionListener = (ActionListener<WritePipelineResponse>) listener;
            new HttpPutPipelineAction(this, (PutPipelineAction) action).execute((PutPipelineRequest) request, actionListener);
        } else if (GetPipelineAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.ingest.GetPipelineAction
            @SuppressWarnings("unchecked")
            final ActionListener<GetPipelineResponse> actionListener = (ActionListener<GetPipelineResponse>) listener;
            new HttpGetPipelineAction(this, (GetPipelineAction) action).execute((GetPipelineRequest) request, actionListener);
        } else {

            // org.elasticsearch.action.ingest.SimulatePipelineAction
            // org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryAction
            // org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryAction
            // org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesAction
            // org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryAction
            // org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction
            // org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusAction
            // org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction
            // org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsAction
            // org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotAction

            // org.elasticsearch.action.admin.cluster.stats.ClusterStatsAction

            // org.elasticsearch.action.admin.cluster.state.ClusterStateAction
            // org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainAction
            // org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteAction
            // org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsAction
            // org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksAction
            // org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction
            // org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskAction
            // org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction
            // org.elasticsearch.action.admin.cluster.node.usage.NodesUsageAction
            // org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction
            // org.elasticsearch.action.admin.indices.segments.IndicesSegmentsAction
            // org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsAction
            // org.elasticsearch.action.admin.indices.stats.IndicesStatsAction
            // org.elasticsearch.action.admin.indices.upgrade.post.UpgradeSettingsAction
            // org.elasticsearch.action.admin.indices.upgrade.post.UpgradeAction
            // org.elasticsearch.action.admin.indices.upgrade.get.UpgradeStatusAction
            // org.elasticsearch.action.admin.indices.recovery.RecoveryAction
            // org.elasticsearch.action.admin.indices.analyze.AnalyzeAction
            // org.elasticsearch.action.admin.indices.shrink.ResizeAction

            // org.elasticsearch.action.termvectors.MultiTermVectorsAction
            // org.elasticsearch.action.termvectors.TermVectorsAction
            // org.elasticsearch.action.admin.indices.shards.IndicesShardStoresAction
            // org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction
            // org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction
            // org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateAction
            // org.elasticsearch.action.admin.cluster.remote.RemoteInfoAction
            // org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptAction
            // org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptAction
            // org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptAction

            throw new UnsupportedOperationException("Action: " + action.name());
        }
    }

    protected String getHost() {
        return hosts[0];
    }

    public CurlRequest getCurlRequest(final Function<String, CurlRequest> method, final String path, final String... indices) {
        return getCurlRequest(method, ContentType.JSON, path, indices);
    }

    public CurlRequest getCurlRequest(final Function<String, CurlRequest> method, final ContentType contentType, final String path,
            final String... indices) {
        final StringBuilder buf = new StringBuilder(100);
        buf.append(getHost());
        if (indices.length > 0) {
            buf.append('/').append(String.join(",", indices));
        }
        if (path != null) {
            buf.append(path);
        }
        // TODO other request headers
        // TODO threadPool
        return method.apply(buf.toString()).header("Content-Type", contentType.getString()).threadPool(ForkJoinPool.commonPool());
    }
}
