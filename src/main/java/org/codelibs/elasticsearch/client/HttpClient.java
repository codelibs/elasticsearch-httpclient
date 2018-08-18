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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.logging.Logger;

import org.codelibs.curl.Curl;
import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.action.HttpAliasesExistAction;
import org.codelibs.elasticsearch.client.action.HttpBulkAction;
import org.codelibs.elasticsearch.client.action.HttpClearIndicesCacheAction;
import org.codelibs.elasticsearch.client.action.HttpClearScrollAction;
import org.codelibs.elasticsearch.client.action.HttpCloseIndexAction;
import org.codelibs.elasticsearch.client.action.HttpClusterHealthAction;
import org.codelibs.elasticsearch.client.action.HttpClusterUpdateSettingsAction;
import org.codelibs.elasticsearch.client.action.HttpCreateIndexAction;
import org.codelibs.elasticsearch.client.action.HttpDeleteAction;
import org.codelibs.elasticsearch.client.action.HttpDeleteIndexAction;
import org.codelibs.elasticsearch.client.action.HttpDeletePipelineAction;
import org.codelibs.elasticsearch.client.action.HttpDeleteStoredScriptAction;
import org.codelibs.elasticsearch.client.action.HttpExplainAction;
import org.codelibs.elasticsearch.client.action.HttpFieldCapabilitiesAction;
import org.codelibs.elasticsearch.client.action.HttpFlushAction;
import org.codelibs.elasticsearch.client.action.HttpForceMergeAction;
import org.codelibs.elasticsearch.client.action.HttpGetAction;
import org.codelibs.elasticsearch.client.action.HttpGetAliasesAction;
import org.codelibs.elasticsearch.client.action.HttpGetFieldMappingsAction;
import org.codelibs.elasticsearch.client.action.HttpGetIndexAction;
import org.codelibs.elasticsearch.client.action.HttpGetMappingsAction;
import org.codelibs.elasticsearch.client.action.HttpGetPipelineAction;
import org.codelibs.elasticsearch.client.action.HttpGetSettingsAction;
import org.codelibs.elasticsearch.client.action.HttpGetStoredScriptAction;
import org.codelibs.elasticsearch.client.action.HttpIndexAction;
import org.codelibs.elasticsearch.client.action.HttpIndicesAliasesAction;
import org.codelibs.elasticsearch.client.action.HttpIndicesExistsAction;
import org.codelibs.elasticsearch.client.action.HttpMainAction;
import org.codelibs.elasticsearch.client.action.HttpMultiGetAction;
import org.codelibs.elasticsearch.client.action.HttpMultiSearchAction;
import org.codelibs.elasticsearch.client.action.HttpOpenIndexAction;
import org.codelibs.elasticsearch.client.action.HttpPendingClusterTasksAction;
import org.codelibs.elasticsearch.client.action.HttpPutMappingAction;
import org.codelibs.elasticsearch.client.action.HttpPutPipelineAction;
import org.codelibs.elasticsearch.client.action.HttpPutStoredScriptAction;
import org.codelibs.elasticsearch.client.action.HttpRefreshAction;
import org.codelibs.elasticsearch.client.action.HttpRolloverAction;
import org.codelibs.elasticsearch.client.action.HttpSearchAction;
import org.codelibs.elasticsearch.client.action.HttpSearchScrollAction;
import org.codelibs.elasticsearch.client.action.HttpShrinkAction;
import org.codelibs.elasticsearch.client.action.HttpSyncedFlushAction;
import org.codelibs.elasticsearch.client.action.HttpTypesExistsAction;
import org.codelibs.elasticsearch.client.action.HttpUpdateAction;
import org.codelibs.elasticsearch.client.action.HttpUpdateSettingsAction;
import org.codelibs.elasticsearch.client.action.HttpValidateQueryAction;
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
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptAction;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptResponse;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptAction;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptAction;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptResponse;
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

    protected String[] hosts;

    protected final Map<Action<?, ?, ?>, BiConsumer<ActionRequest, ActionListener<?>>> actions = new HashMap<>();

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

        actions.put(SearchAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.search.SearchAction
                @SuppressWarnings("unchecked")
                final ActionListener<SearchResponse> actionListener = (ActionListener<SearchResponse>) listener;
                new HttpSearchAction(this, SearchAction.INSTANCE).execute((SearchRequest) request, actionListener);
            });
        actions.put(RefreshAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.indices.refresh.RefreshAction
                @SuppressWarnings("unchecked")
                final ActionListener<RefreshResponse> actionListener = (ActionListener<RefreshResponse>) listener;
                new HttpRefreshAction(this, RefreshAction.INSTANCE).execute((RefreshRequest) request, actionListener);
            });
        actions.put(CreateIndexAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.indices.create.CreateIndexAction
                @SuppressWarnings("unchecked")
                final ActionListener<CreateIndexResponse> actionListener = (ActionListener<CreateIndexResponse>) listener;
                new HttpCreateIndexAction(this, CreateIndexAction.INSTANCE).execute((CreateIndexRequest) request, actionListener);
            });
        actions.put(DeleteIndexAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.indices.delete.DeleteIndexAction
                @SuppressWarnings("unchecked")
                final ActionListener<DeleteIndexResponse> actionListener = (ActionListener<DeleteIndexResponse>) listener;
                new HttpDeleteIndexAction(this, DeleteIndexAction.INSTANCE).execute((DeleteIndexRequest) request, actionListener);
            });
        actions.put(GetIndexAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.indices.get.GetIndexAction
                @SuppressWarnings("unchecked")
                final ActionListener<GetIndexResponse> actionListener = (ActionListener<GetIndexResponse>) listener;
                new HttpGetIndexAction(this, GetIndexAction.INSTANCE).execute((GetIndexRequest) request, actionListener);
            });
        actions.put(OpenIndexAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.indices.open.OpenIndexAction
                @SuppressWarnings("unchecked")
                final ActionListener<OpenIndexResponse> actionListener = (ActionListener<OpenIndexResponse>) listener;
                new HttpOpenIndexAction(this, OpenIndexAction.INSTANCE).execute((OpenIndexRequest) request, actionListener);
            });
        actions.put(CloseIndexAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.indices.close.CloseIndexAction
                @SuppressWarnings("unchecked")
                final ActionListener<CloseIndexResponse> actionListener = (ActionListener<CloseIndexResponse>) listener;
                new HttpCloseIndexAction(this, CloseIndexAction.INSTANCE).execute((CloseIndexRequest) request, actionListener);
            });
        actions.put(IndicesExistsAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction
                @SuppressWarnings("unchecked")
                final ActionListener<IndicesExistsResponse> actionListener = (ActionListener<IndicesExistsResponse>) listener;
                new HttpIndicesExistsAction(this, IndicesExistsAction.INSTANCE).execute((IndicesExistsRequest) request, actionListener);
            });
        actions.put(IndicesAliasesAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction
                @SuppressWarnings("unchecked")
                final ActionListener<IndicesAliasesResponse> actionListener = (ActionListener<IndicesAliasesResponse>) listener;
                new HttpIndicesAliasesAction(this, IndicesAliasesAction.INSTANCE).execute((IndicesAliasesRequest) request, actionListener);
            });
        actions.put(PutMappingAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction
                @SuppressWarnings("unchecked")
                final ActionListener<PutMappingResponse> actionListener = (ActionListener<PutMappingResponse>) listener;
                new HttpPutMappingAction(this, PutMappingAction.INSTANCE).execute((PutMappingRequest) request, actionListener);
            });
        actions.put(GetMappingsAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction
                @SuppressWarnings("unchecked")
                final ActionListener<GetMappingsResponse> actionListener = (ActionListener<GetMappingsResponse>) listener;
                new HttpGetMappingsAction(this, GetMappingsAction.INSTANCE).execute((GetMappingsRequest) request, actionListener);
            });
        actions.put(GetFieldMappingsAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction
                @SuppressWarnings("unchecked")
                final ActionListener<GetFieldMappingsResponse> actionListener = (ActionListener<GetFieldMappingsResponse>) listener;
                new HttpGetFieldMappingsAction(this, GetFieldMappingsAction.INSTANCE).execute((GetFieldMappingsRequest) request,
                        actionListener);
            });
        actions.put(FlushAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.indices.flush.FlushAction
                @SuppressWarnings("unchecked")
                final ActionListener<FlushResponse> actionListener = (ActionListener<FlushResponse>) listener;
                new HttpFlushAction(this, FlushAction.INSTANCE).execute((FlushRequest) request, actionListener);
            });
        actions.put(ClearScrollAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.search.ClearScrollAction
                @SuppressWarnings("unchecked")
                final ActionListener<ClearScrollResponse> actionListener = (ActionListener<ClearScrollResponse>) listener;
                new HttpClearScrollAction(this, ClearScrollAction.INSTANCE).execute((ClearScrollRequest) request, actionListener);
            });
        actions.put(MultiSearchAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.search.MultiSearchAction
                @SuppressWarnings("unchecked")
                final ActionListener<MultiSearchResponse> actionListener = (ActionListener<MultiSearchResponse>) listener;
                new HttpMultiSearchAction(this, MultiSearchAction.INSTANCE).execute((MultiSearchRequest) request, actionListener);
            });
        actions.put(SearchScrollAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.search.MultiSearchAction
                @SuppressWarnings("unchecked")
                final ActionListener<SearchResponse> actionListener = (ActionListener<SearchResponse>) listener;
                new HttpSearchScrollAction(this, SearchScrollAction.INSTANCE).execute((SearchScrollRequest) request, actionListener);
            });
        actions.put(IndexAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.index.IndexAction
                @SuppressWarnings("unchecked")
                final ActionListener<IndexResponse> actionListener = (ActionListener<IndexResponse>) listener;
                new HttpIndexAction(this, IndexAction.INSTANCE).execute((IndexRequest) request, actionListener);
            });
        actions.put(FieldCapabilitiesAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.fieldcaps.FieldCapabilitiesAction)
                @SuppressWarnings("unchecked")
                final ActionListener<FieldCapabilitiesResponse> actionListener = (ActionListener<FieldCapabilitiesResponse>) listener;
                new HttpFieldCapabilitiesAction(this, FieldCapabilitiesAction.INSTANCE).execute((FieldCapabilitiesRequest) request,
                        actionListener);
            });
        actions.put(GetAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.get.GetAction
                @SuppressWarnings("unchecked")
                final ActionListener<GetResponse> actionListener = (ActionListener<GetResponse>) listener;
                new HttpGetAction(this, GetAction.INSTANCE).execute((GetRequest) request, actionListener);
            });
        actions.put(MultiGetAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.get.MultiGetAction
                @SuppressWarnings("unchecked")
                final ActionListener<MultiGetResponse> actionListener = (ActionListener<MultiGetResponse>) listener;
                new HttpMultiGetAction(this, MultiGetAction.INSTANCE).execute((MultiGetRequest) request, actionListener);
            });
        actions.put(UpdateAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.update.UpdateAction
                @SuppressWarnings("unchecked")
                final ActionListener<UpdateResponse> actionListener = (ActionListener<UpdateResponse>) listener;
                new HttpUpdateAction(this, UpdateAction.INSTANCE).execute((UpdateRequest) request, actionListener);
            });
        actions.put(BulkAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.bulk.BulkAction
                @SuppressWarnings("unchecked")
                final ActionListener<BulkResponse> actionListener = (ActionListener<BulkResponse>) listener;
                new HttpBulkAction(this, BulkAction.INSTANCE).execute((BulkRequest) request, actionListener);
            });
        actions.put(DeleteAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.delete.DeleteAction
                @SuppressWarnings("unchecked")
                final ActionListener<DeleteResponse> actionListener = (ActionListener<DeleteResponse>) listener;
                new HttpDeleteAction(this, DeleteAction.INSTANCE).execute((DeleteRequest) request, actionListener);
            });
        actions.put(ExplainAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.explain.ExplainAction
                @SuppressWarnings("unchecked")
                final ActionListener<ExplainResponse> actionListener = (ActionListener<ExplainResponse>) listener;
                new HttpExplainAction(this, ExplainAction.INSTANCE).execute((ExplainRequest) request, actionListener);
            });
        actions.put(UpdateSettingsAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction
                @SuppressWarnings("unchecked")
                final ActionListener<UpdateSettingsResponse> actionListener = (ActionListener<UpdateSettingsResponse>) listener;
                new HttpUpdateSettingsAction(this, UpdateSettingsAction.INSTANCE).execute((UpdateSettingsRequest) request, actionListener);
            });
        actions.put(GetSettingsAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction
                @SuppressWarnings("unchecked")
                final ActionListener<GetSettingsResponse> actionListener = (ActionListener<GetSettingsResponse>) listener;
                new HttpGetSettingsAction(this, GetSettingsAction.INSTANCE).execute((GetSettingsRequest) request, actionListener);
            });
        actions.put(ForceMergeAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction
                @SuppressWarnings("unchecked")
                final ActionListener<ForceMergeResponse> actionListener = (ActionListener<ForceMergeResponse>) listener;
                new HttpForceMergeAction(this, ForceMergeAction.INSTANCE).execute((ForceMergeRequest) request, actionListener);
            });
        actions.put(MainAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.main.MainAction
                @SuppressWarnings("unchecked")
                final ActionListener<MainResponse> actionListener = (ActionListener<MainResponse>) listener;
                new HttpMainAction(this, MainAction.INSTANCE).execute((MainRequest) request, actionListener);
            });
        actions.put(ClusterUpdateSettingsAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction
                @SuppressWarnings("unchecked")
                final ActionListener<ClusterUpdateSettingsResponse> actionListener =
                        (ActionListener<ClusterUpdateSettingsResponse>) listener;
                new HttpClusterUpdateSettingsAction(this, ClusterUpdateSettingsAction.INSTANCE).execute(
                        (ClusterUpdateSettingsRequest) request, actionListener);
            });
        actions.put(ClusterHealthAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.cluster.health.ClusterHealthAction
                @SuppressWarnings("unchecked")
                final ActionListener<ClusterHealthResponse> actionListener = (ActionListener<ClusterHealthResponse>) listener;
                new HttpClusterHealthAction(this, ClusterHealthAction.INSTANCE).execute((ClusterHealthRequest) request, actionListener);
            });
        actions.put(AliasesExistAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.indices.alias.exists.AliasesExistAction
                @SuppressWarnings("unchecked")
                final ActionListener<AliasesExistResponse> actionListener = (ActionListener<AliasesExistResponse>) listener;
                new HttpAliasesExistAction(this, AliasesExistAction.INSTANCE).execute((GetAliasesRequest) request, actionListener);
            });
        actions.put(ValidateQueryAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction
                @SuppressWarnings("unchecked")
                final ActionListener<ValidateQueryResponse> actionListener = (ActionListener<ValidateQueryResponse>) listener;
                new HttpValidateQueryAction(this, ValidateQueryAction.INSTANCE).execute((ValidateQueryRequest) request, actionListener);
            });
        actions.put(PendingClusterTasksAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksAction
                @SuppressWarnings("unchecked")
                final ActionListener<PendingClusterTasksResponse> actionListener = (ActionListener<PendingClusterTasksResponse>) listener;
                new HttpPendingClusterTasksAction(this, PendingClusterTasksAction.INSTANCE).execute((PendingClusterTasksRequest) request,
                        actionListener);
            });
        actions.put(GetAliasesAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction
                @SuppressWarnings("unchecked")
                final ActionListener<GetAliasesResponse> actionListener = (ActionListener<GetAliasesResponse>) listener;
                new HttpGetAliasesAction(this, GetAliasesAction.INSTANCE).execute((GetAliasesRequest) request, actionListener);
            });
        actions.put(SyncedFlushAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.indices.flush.SyncedFlushAction
                @SuppressWarnings("unchecked")
                final ActionListener<SyncedFlushResponse> actionListener = (ActionListener<SyncedFlushResponse>) listener;
                new HttpSyncedFlushAction(this, SyncedFlushAction.INSTANCE).execute((SyncedFlushRequest) request, actionListener);
            });
        actions.put(ShrinkAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.indices.shrink.ShrinkAction
                @SuppressWarnings("unchecked")
                final ActionListener<ResizeResponse> actionListener = (ActionListener<ResizeResponse>) listener;
                new HttpShrinkAction(this, ShrinkAction.INSTANCE).execute((ResizeRequest) request, actionListener);
            });
        actions.put(TypesExistsAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.indices.exists.types.TypesExistsAction
                @SuppressWarnings("unchecked")
                final ActionListener<TypesExistsResponse> actionListener = (ActionListener<TypesExistsResponse>) listener;
                new HttpTypesExistsAction(this, TypesExistsAction.INSTANCE).execute((TypesExistsRequest) request, actionListener);
            });
        actions.put(RolloverAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.indices.rollover.RolloverAction
                @SuppressWarnings("unchecked")
                final ActionListener<RolloverResponse> actionListener = (ActionListener<RolloverResponse>) listener;
                new HttpRolloverAction(this, RolloverAction.INSTANCE).execute((RolloverRequest) request, actionListener);
            });
        actions.put(ClearIndicesCacheAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheAction
                @SuppressWarnings("unchecked")
                final ActionListener<ClearIndicesCacheResponse> actionListener = (ActionListener<ClearIndicesCacheResponse>) listener;
                new HttpClearIndicesCacheAction(this, ClearIndicesCacheAction.INSTANCE).execute((ClearIndicesCacheRequest) request,
                        actionListener);
            });
        actions.put(PutPipelineAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.ingest.PutPipelineAction
                @SuppressWarnings("unchecked")
                final ActionListener<WritePipelineResponse> actionListener = (ActionListener<WritePipelineResponse>) listener;
                new HttpPutPipelineAction(this, PutPipelineAction.INSTANCE).execute((PutPipelineRequest) request, actionListener);
            });
        actions.put(GetPipelineAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.ingest.GetPipelineAction
                @SuppressWarnings("unchecked")
                final ActionListener<GetPipelineResponse> actionListener = (ActionListener<GetPipelineResponse>) listener;
                new HttpGetPipelineAction(this, GetPipelineAction.INSTANCE).execute((GetPipelineRequest) request, actionListener);
            });
        actions.put(DeletePipelineAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.ingest.DeletePipelineAction
                @SuppressWarnings("unchecked")
                final ActionListener<WritePipelineResponse> actionListener = (ActionListener<WritePipelineResponse>) listener;
                new HttpDeletePipelineAction(this, DeletePipelineAction.INSTANCE).execute((DeletePipelineRequest) request, actionListener);
            });
        actions.put(PutStoredScriptAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptAction
                @SuppressWarnings("unchecked")
                final ActionListener<PutStoredScriptResponse> actionListener = (ActionListener<PutStoredScriptResponse>) listener;
                new HttpPutStoredScriptAction(this, PutStoredScriptAction.INSTANCE).execute((PutStoredScriptRequest) request,
                        actionListener);
            });
        actions.put(GetStoredScriptAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptAction
                @SuppressWarnings("unchecked")
                final ActionListener<GetStoredScriptResponse> actionListener = (ActionListener<GetStoredScriptResponse>) listener;
                new HttpGetStoredScriptAction(this, GetStoredScriptAction.INSTANCE).execute((GetStoredScriptRequest) request,
                        actionListener);
            });
        actions.put(DeleteStoredScriptAction.INSTANCE,
                (request, listener) -> {
                    // org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptAction
                @SuppressWarnings("unchecked")
                final ActionListener<DeleteStoredScriptResponse> actionListener = (ActionListener<DeleteStoredScriptResponse>) listener;
                new HttpDeleteStoredScriptAction(this, DeleteStoredScriptAction.INSTANCE).execute((DeleteStoredScriptRequest) request,
                        actionListener);
            });

        // org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction
        // org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction
        // org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateAction

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

        // org.elasticsearch.action.ingest.SimulatePipelineAction
        // org.elasticsearch.action.termvectors.MultiTermVectorsAction
        // org.elasticsearch.action.termvectors.TermVectorsAction
        // org.elasticsearch.action.admin.indices.shards.IndicesShardStoresAction
        // org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryAction
        // org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryAction
        // org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesAction
        // org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryAction
        // org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction
        // org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusAction
        // org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction
        // org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsAction
        // org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotAction
        // org.elasticsearch.action.admin.cluster.remote.RemoteInfoAction

    }

    @Override
    public void close() {
        // TODO thread pool management
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(
            final Action<Request, Response, RequestBuilder> action, final Request request, final ActionListener<Response> listener) {
        final BiConsumer<ActionRequest, ActionListener<?>> httpAction = actions.get(action);
        if (httpAction == null) {
            throw new UnsupportedOperationException("Action: " + action.name());
        }
        httpAction.accept(request, listener);
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
