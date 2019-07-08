/*
 * Copyright 2012-2019 CodeLibs Project and the Others.
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

import static java.util.stream.Collectors.toList;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codelibs.curl.Curl;
import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.action.HttpAliasesExistAction;
import org.codelibs.elasticsearch.client.action.HttpAnalyzeAction;
import org.codelibs.elasticsearch.client.action.HttpBulkAction;
import org.codelibs.elasticsearch.client.action.HttpCancelTasksAction;
import org.codelibs.elasticsearch.client.action.HttpClearIndicesCacheAction;
import org.codelibs.elasticsearch.client.action.HttpClearScrollAction;
import org.codelibs.elasticsearch.client.action.HttpCloseIndexAction;
import org.codelibs.elasticsearch.client.action.HttpClusterHealthAction;
import org.codelibs.elasticsearch.client.action.HttpClusterRerouteAction;
import org.codelibs.elasticsearch.client.action.HttpClusterUpdateSettingsAction;
import org.codelibs.elasticsearch.client.action.HttpCreateIndexAction;
import org.codelibs.elasticsearch.client.action.HttpCreateSnapshotAction;
import org.codelibs.elasticsearch.client.action.HttpDeleteAction;
import org.codelibs.elasticsearch.client.action.HttpDeleteIndexAction;
import org.codelibs.elasticsearch.client.action.HttpDeleteIndexTemplateAction;
import org.codelibs.elasticsearch.client.action.HttpDeletePipelineAction;
import org.codelibs.elasticsearch.client.action.HttpDeleteRepositoryAction;
import org.codelibs.elasticsearch.client.action.HttpDeleteSnapshotAction;
import org.codelibs.elasticsearch.client.action.HttpDeleteStoredScriptAction;
import org.codelibs.elasticsearch.client.action.HttpExplainAction;
import org.codelibs.elasticsearch.client.action.HttpFieldCapabilitiesAction;
import org.codelibs.elasticsearch.client.action.HttpFlushAction;
import org.codelibs.elasticsearch.client.action.HttpForceMergeAction;
import org.codelibs.elasticsearch.client.action.HttpGetAction;
import org.codelibs.elasticsearch.client.action.HttpGetAliasesAction;
import org.codelibs.elasticsearch.client.action.HttpGetFieldMappingsAction;
import org.codelibs.elasticsearch.client.action.HttpGetIndexAction;
import org.codelibs.elasticsearch.client.action.HttpGetIndexTemplatesAction;
import org.codelibs.elasticsearch.client.action.HttpGetMappingsAction;
import org.codelibs.elasticsearch.client.action.HttpGetPipelineAction;
import org.codelibs.elasticsearch.client.action.HttpGetRepositoriesAction;
import org.codelibs.elasticsearch.client.action.HttpGetSettingsAction;
import org.codelibs.elasticsearch.client.action.HttpGetSnapshotsAction;
import org.codelibs.elasticsearch.client.action.HttpGetStoredScriptAction;
import org.codelibs.elasticsearch.client.action.HttpIndexAction;
import org.codelibs.elasticsearch.client.action.HttpIndicesAliasesAction;
import org.codelibs.elasticsearch.client.action.HttpIndicesExistsAction;
import org.codelibs.elasticsearch.client.action.HttpListTasksAction;
import org.codelibs.elasticsearch.client.action.HttpMainAction;
import org.codelibs.elasticsearch.client.action.HttpMultiGetAction;
import org.codelibs.elasticsearch.client.action.HttpMultiSearchAction;
import org.codelibs.elasticsearch.client.action.HttpNodesStatsAction;
import org.codelibs.elasticsearch.client.action.HttpOpenIndexAction;
import org.codelibs.elasticsearch.client.action.HttpPendingClusterTasksAction;
import org.codelibs.elasticsearch.client.action.HttpPutIndexTemplateAction;
import org.codelibs.elasticsearch.client.action.HttpPutMappingAction;
import org.codelibs.elasticsearch.client.action.HttpPutPipelineAction;
import org.codelibs.elasticsearch.client.action.HttpPutRepositoryAction;
import org.codelibs.elasticsearch.client.action.HttpPutStoredScriptAction;
import org.codelibs.elasticsearch.client.action.HttpRefreshAction;
import org.codelibs.elasticsearch.client.action.HttpRestoreSnapshotAction;
import org.codelibs.elasticsearch.client.action.HttpRolloverAction;
import org.codelibs.elasticsearch.client.action.HttpSearchAction;
import org.codelibs.elasticsearch.client.action.HttpSearchScrollAction;
import org.codelibs.elasticsearch.client.action.HttpShrinkAction;
import org.codelibs.elasticsearch.client.action.HttpSimulatePipelineAction;
import org.codelibs.elasticsearch.client.action.HttpSnapshotsStatusAction;
import org.codelibs.elasticsearch.client.action.HttpSyncedFlushAction;
import org.codelibs.elasticsearch.client.action.HttpUpdateAction;
import org.codelibs.elasticsearch.client.action.HttpUpdateSettingsAction;
import org.codelibs.elasticsearch.client.action.HttpValidateQueryAction;
import org.codelibs.elasticsearch.client.action.HttpVerifyRepositoryAction;
import org.codelibs.elasticsearch.client.util.UrlUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesAction;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusAction;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptAction;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptAction;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptAction;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksAction;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksRequest;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistAction;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
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
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
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
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeResponse;
import org.elasticsearch.action.admin.indices.shrink.ShrinkAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.spi.NamedXContentProvider;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.adjacency.AdjacencyMatrixAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.adjacency.ParsedAdjacencyMatrix;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilter;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilters;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.global.ParsedGlobal;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedDateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedHistogram;
import org.elasticsearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.missing.ParsedMissing;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.ParsedNested;
import org.elasticsearch.search.aggregations.bucket.nested.ParsedReverseNested;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.GeoDistanceAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.IpRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.ParsedBinaryRange;
import org.elasticsearch.search.aggregations.bucket.range.ParsedDateRange;
import org.elasticsearch.search.aggregations.bucket.range.ParsedGeoDistance;
import org.elasticsearch.search.aggregations.bucket.range.ParsedRange;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.sampler.InternalSampler;
import org.elasticsearch.search.aggregations.bucket.sampler.ParsedSampler;
import org.elasticsearch.search.aggregations.bucket.significant.ParsedSignificantLongTerms;
import org.elasticsearch.search.aggregations.bucket.significant.ParsedSignificantStringTerms;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantLongTerms;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.DoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedDoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedLongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ExtendedStatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.GeoBoundsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.GeoCentroidAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalHDRPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.InternalHDRPercentiles;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedAvg;
import org.elasticsearch.search.aggregations.metrics.ParsedCardinality;
import org.elasticsearch.search.aggregations.metrics.ParsedExtendedStats;
import org.elasticsearch.search.aggregations.metrics.ParsedGeoBounds;
import org.elasticsearch.search.aggregations.metrics.ParsedGeoCentroid;
import org.elasticsearch.search.aggregations.metrics.ParsedHDRPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.ParsedHDRPercentiles;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.aggregations.metrics.ParsedMin;
import org.elasticsearch.search.aggregations.metrics.ParsedScriptedMetric;
import org.elasticsearch.search.aggregations.metrics.ParsedStats;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.ParsedTDigestPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.ParsedTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.ParsedTopHits;
import org.elasticsearch.search.aggregations.metrics.ParsedValueCount;
import org.elasticsearch.search.aggregations.metrics.ScriptedMetricAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.DerivativePipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.ExtendedStatsBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.InternalBucketMetricValue;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.ParsedBucketMetricValue;
import org.elasticsearch.search.aggregations.pipeline.ParsedDerivative;
import org.elasticsearch.search.aggregations.pipeline.ParsedExtendedStatsBucket;
import org.elasticsearch.search.aggregations.pipeline.ParsedPercentilesBucket;
import org.elasticsearch.search.aggregations.pipeline.ParsedSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.ParsedStatsBucket;
import org.elasticsearch.search.aggregations.pipeline.PercentilesBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.StatsBucketPipelineAggregationBuilder;
import org.elasticsearch.threadpool.ThreadPool;

public class HttpClient extends AbstractClient {

    protected static final Function<String, CurlRequest> GET = Curl::get;

    protected static final Function<String, CurlRequest> POST = Curl::post;

    protected static final Function<String, CurlRequest> PUT = Curl::put;

    protected static final Function<String, CurlRequest> DELETE = Curl::delete;

    protected static final Function<String, CurlRequest> HEAD = Curl::head;

    protected String[] hosts;

    protected final Map<Action<?>, BiConsumer<ActionRequest, ActionListener<?>>> actions = new HashMap<>();

    protected final NamedXContentRegistry namedXContentRegistry;

    protected final ForkJoinPool threadPool;

    protected final String basicAuth;

    protected final List<Function<CurlRequest, CurlRequest>> requestBuilderList = new ArrayList<>();

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
        this(settings, threadPool, Collections.emptyList());
    }

    public HttpClient(final Settings settings, final ThreadPool threadPool, final List<NamedXContentRegistry.Entry> namedXContentEntries) {
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

        basicAuth = createBasicAuthentication(settings);
        this.threadPool = createThreadPool(settings);

        namedXContentRegistry =
                new NamedXContentRegistry(Stream
                        .of(getDefaultNamedXContents().stream(), getProvidedNamedXContents().stream(), namedXContentEntries.stream())
                        .flatMap(Function.identity()).collect(toList()));

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
                final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
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
                final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
                new HttpIndicesAliasesAction(this, IndicesAliasesAction.INSTANCE).execute((IndicesAliasesRequest) request, actionListener);
            });
        actions.put(PutMappingAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction
                @SuppressWarnings("unchecked")
                final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
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
                final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
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
                final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
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
                final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
                new HttpDeletePipelineAction(this, DeletePipelineAction.INSTANCE).execute((DeletePipelineRequest) request, actionListener);
            });
        actions.put(PutStoredScriptAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptAction
                @SuppressWarnings("unchecked")
                final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
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
        actions.put(DeleteStoredScriptAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptAction
                @SuppressWarnings("unchecked")
                final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
                new HttpDeleteStoredScriptAction(this, DeleteStoredScriptAction.INSTANCE).execute((DeleteStoredScriptRequest) request,
                        actionListener);
            });
        actions.put(PutIndexTemplateAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction
                @SuppressWarnings("unchecked")
                final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
                new HttpPutIndexTemplateAction(this, PutIndexTemplateAction.INSTANCE).execute((PutIndexTemplateRequest) request,
                        actionListener);
            });
        actions.put(GetIndexTemplatesAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction
                @SuppressWarnings("unchecked")
                final ActionListener<GetIndexTemplatesResponse> actionListener = (ActionListener<GetIndexTemplatesResponse>) listener;
                new HttpGetIndexTemplatesAction(this, GetIndexTemplatesAction.INSTANCE).execute((GetIndexTemplatesRequest) request,
                        actionListener);
            });
        actions.put(DeleteIndexTemplateAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateAction
                @SuppressWarnings("unchecked")
                final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
                new HttpDeleteIndexTemplateAction(this, DeleteIndexTemplateAction.INSTANCE).execute((DeleteIndexTemplateRequest) request,
                        actionListener);
            });
        actions.put(CancelTasksAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksAction
                @SuppressWarnings("unchecked")
                final ActionListener<CancelTasksResponse> actionListener = (ActionListener<CancelTasksResponse>) listener;
                new HttpCancelTasksAction(this, CancelTasksAction.INSTANCE).execute((CancelTasksRequest) request, actionListener);
            });
        actions.put(ListTasksAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction
                @SuppressWarnings("unchecked")
                final ActionListener<ListTasksResponse> actionListener = (ActionListener<ListTasksResponse>) listener;
                new HttpListTasksAction(this, ListTasksAction.INSTANCE).execute((ListTasksRequest) request, actionListener);
            });
        actions.put(VerifyRepositoryAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryAction
                @SuppressWarnings("unchecked")
                final ActionListener<VerifyRepositoryResponse> actionListener = (ActionListener<VerifyRepositoryResponse>) listener;
                new HttpVerifyRepositoryAction(this, VerifyRepositoryAction.INSTANCE).execute((VerifyRepositoryRequest) request,
                        actionListener);
            });
        actions.put(PutRepositoryAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryAction
                @SuppressWarnings("unchecked")
                final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
                new HttpPutRepositoryAction(this, PutRepositoryAction.INSTANCE).execute((PutRepositoryRequest) request, actionListener);
            });
        actions.put(GetRepositoriesAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesAction
                @SuppressWarnings("unchecked")
                final ActionListener<GetRepositoriesResponse> actionListener = (ActionListener<GetRepositoriesResponse>) listener;
                new HttpGetRepositoriesAction(this, GetRepositoriesAction.INSTANCE).execute((GetRepositoriesRequest) request,
                        actionListener);
            });
        actions.put(DeleteRepositoryAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryAction
                @SuppressWarnings("unchecked")
                final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
                new HttpDeleteRepositoryAction(this, DeleteRepositoryAction.INSTANCE).execute((DeleteRepositoryRequest) request,
                        actionListener);
            });
        actions.put(AnalyzeAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.indices.analyze.AnalyzeAction
                @SuppressWarnings("unchecked")
                final ActionListener<AnalyzeResponse> actionListener = (ActionListener<AnalyzeResponse>) listener;
                new HttpAnalyzeAction(this, AnalyzeAction.INSTANCE).execute((AnalyzeRequest) request, actionListener);
            });
        actions.put(SimulatePipelineAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.ingest.SimulatePipelineAction
                @SuppressWarnings("unchecked")
                final ActionListener<SimulatePipelineResponse> actionListener = (ActionListener<SimulatePipelineResponse>) listener;
                new HttpSimulatePipelineAction(this, SimulatePipelineAction.INSTANCE).execute((SimulatePipelineRequest) request,
                        actionListener);
            });
        actions.put(SnapshotsStatusAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusAction
                @SuppressWarnings("unchecked")
                final ActionListener<SnapshotsStatusResponse> actionListener = (ActionListener<SnapshotsStatusResponse>) listener;
                new HttpSnapshotsStatusAction(this, SnapshotsStatusAction.INSTANCE).execute((SnapshotsStatusRequest) request,
                        actionListener);
            });
        actions.put(CreateSnapshotAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction
                @SuppressWarnings("unchecked")
                final ActionListener<CreateSnapshotResponse> actionListener = (ActionListener<CreateSnapshotResponse>) listener;
                new HttpCreateSnapshotAction(this, CreateSnapshotAction.INSTANCE).execute((CreateSnapshotRequest) request, actionListener);
            });
        actions.put(GetSnapshotsAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsAction
                @SuppressWarnings("unchecked")
                final ActionListener<GetSnapshotsResponse> actionListener = (ActionListener<GetSnapshotsResponse>) listener;
                new HttpGetSnapshotsAction(this, GetSnapshotsAction.INSTANCE).execute((GetSnapshotsRequest) request, actionListener);
            });
        actions.put(DeleteSnapshotAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotAction
                @SuppressWarnings("unchecked")
                final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
                new HttpDeleteSnapshotAction(this, DeleteSnapshotAction.INSTANCE).execute((DeleteSnapshotRequest) request, actionListener);
            });
        actions.put(ClusterRerouteAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteAction
                @SuppressWarnings("unchecked")
                final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
                new HttpClusterRerouteAction(this, ClusterRerouteAction.INSTANCE).execute((ClusterRerouteRequest) request, actionListener);
            });
        actions.put(RestoreSnapshotAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction
                @SuppressWarnings("unchecked")
                final ActionListener<RestoreSnapshotResponse> actionListener = (ActionListener<RestoreSnapshotResponse>) listener;
                new HttpRestoreSnapshotAction(this, RestoreSnapshotAction.INSTANCE).execute((RestoreSnapshotRequest) request,
                        actionListener);
            });
        actions.put(NodesStatsAction.INSTANCE, (request, listener) -> {
            // org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction
                @SuppressWarnings("unchecked")
                final ActionListener<NodesStatsResponse> actionListener = (ActionListener<NodesStatsResponse>) listener;
                new HttpNodesStatsAction(this, NodesStatsAction.INSTANCE).execute((NodesStatsRequest) request, actionListener);
            });

        // org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainAction
        // org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsAction
        // org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskAction
        // org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction
        // org.elasticsearch.action.admin.cluster.node.usage.NodesUsageAction
        // org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction
        // org.elasticsearch.action.admin.cluster.remote.RemoteInfoAction
        // org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsAction
        // org.elasticsearch.action.admin.cluster.state.ClusterStateAction
        // org.elasticsearch.action.admin.cluster.stats.ClusterStatsAction
        // org.elasticsearch.action.admin.indices.recovery.RecoveryAction
        // org.elasticsearch.action.admin.indices.segments.IndicesSegmentsAction
        // org.elasticsearch.action.admin.indices.shards.IndicesShardStoresActions
        // org.elasticsearch.action.admin.indices.stats.IndicesStatsAction
        // org.elasticsearch.action.admin.indices.upgrade.get.UpgradeStatusAction
        // org.elasticsearch.action.admin.indices.upgrade.post.UpgradeAction
        // org.elasticsearch.action.admin.indices.upgrade.post.UpgradeSettingsAction
        // org.elasticsearch.action.termvectors.MultiTermVectorsAction
        // org.elasticsearch.action.termvectors.TermVectorsAction

    }

    private String createBasicAuthentication(final Settings settings) {
        final String username = settings.get("elasticsearch.username");
        final String password = settings.get("elasticsearch.password");
        if (username != null && password != null) {
            final String value = username + ":" + password;
            return "Basic " + java.util.Base64.getEncoder().encodeToString(value.toString().getBytes(StandardCharsets.UTF_8));
        }
        return null;
    }

    @Override
    public void close() {
        if (!threadPool.isShutdown()) {
            try {
                threadPool.shutdown();
                threadPool.awaitTermination(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // nothing
            } finally {
                threadPool.shutdownNow();
            }
        }
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(Action<Response> action, Request request,
            ActionListener<Response> listener) {
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
            buf.append('/').append(UrlUtils.joinAndEncode(",", indices));
        }
        if (path != null) {
            buf.append(path);
        }
        CurlRequest request = method.apply(buf.toString()).header("Content-Type", contentType.getString()).threadPool(threadPool);
        if (basicAuth != null) {
            request = request.header("Authorization", basicAuth);
        }
        for (final Function<CurlRequest, CurlRequest> builder : requestBuilderList) {
            request = builder.apply(request);
        }
        return request;
    }

    protected ForkJoinPool createThreadPool(final Settings settings) {
        int parallelism = settings.getAsInt("thread_pool.http.size", Runtime.getRuntime().availableProcessors());
        boolean asyncMode = settings.getAsBoolean("thread_pool.http.async", false);
        return new ForkJoinPool(parallelism, pool -> new WorkerThread(pool), (t, e) -> logger.warn("An exception has been raised by {}",
                t.getName(), e), asyncMode);
    }

    protected List<NamedXContentRegistry.Entry> getDefaultNamedXContents() {
        // TODO check SearchModule
        final Map<String, ContextParser<Object, ? extends Aggregation>> map = new HashMap<>();
        map.put(CardinalityAggregationBuilder.NAME, (p, c) -> ParsedCardinality.fromXContent(p, (String) c));
        map.put(InternalHDRPercentiles.NAME, (p, c) -> ParsedHDRPercentiles.fromXContent(p, (String) c));
        map.put(InternalHDRPercentileRanks.NAME, (p, c) -> ParsedHDRPercentileRanks.fromXContent(p, (String) c));
        map.put(InternalTDigestPercentiles.NAME, (p, c) -> ParsedTDigestPercentiles.fromXContent(p, (String) c));
        map.put(InternalTDigestPercentileRanks.NAME, (p, c) -> ParsedTDigestPercentileRanks.fromXContent(p, (String) c));
        map.put(PercentilesBucketPipelineAggregationBuilder.NAME, (p, c) -> ParsedPercentilesBucket.fromXContent(p, (String) c));
        map.put(MinAggregationBuilder.NAME, (p, c) -> ParsedMin.fromXContent(p, (String) c));
        map.put(MaxAggregationBuilder.NAME, (p, c) -> ParsedMax.fromXContent(p, (String) c));
        map.put(SumAggregationBuilder.NAME, (p, c) -> ParsedSum.fromXContent(p, (String) c));
        map.put(AvgAggregationBuilder.NAME, (p, c) -> ParsedAvg.fromXContent(p, (String) c));
        map.put(ValueCountAggregationBuilder.NAME, (p, c) -> ParsedValueCount.fromXContent(p, (String) c));
        map.put(InternalSimpleValue.NAME, (p, c) -> ParsedSimpleValue.fromXContent(p, (String) c));
        map.put(DerivativePipelineAggregationBuilder.NAME, (p, c) -> ParsedDerivative.fromXContent(p, (String) c));
        map.put(InternalBucketMetricValue.NAME, (p, c) -> ParsedBucketMetricValue.fromXContent(p, (String) c));
        map.put(StatsAggregationBuilder.NAME, (p, c) -> ParsedStats.fromXContent(p, (String) c));
        map.put(StatsBucketPipelineAggregationBuilder.NAME, (p, c) -> ParsedStatsBucket.fromXContent(p, (String) c));
        map.put(ExtendedStatsAggregationBuilder.NAME, (p, c) -> ParsedExtendedStats.fromXContent(p, (String) c));
        map.put(ExtendedStatsBucketPipelineAggregationBuilder.NAME, (p, c) -> ParsedExtendedStatsBucket.fromXContent(p, (String) c));
        map.put(GeoBoundsAggregationBuilder.NAME, (p, c) -> ParsedGeoBounds.fromXContent(p, (String) c));
        map.put(GeoCentroidAggregationBuilder.NAME, (p, c) -> ParsedGeoCentroid.fromXContent(p, (String) c));
        map.put(HistogramAggregationBuilder.NAME, (p, c) -> ParsedHistogram.fromXContent(p, (String) c));
        map.put(DateHistogramAggregationBuilder.NAME, (p, c) -> ParsedDateHistogram.fromXContent(p, (String) c));
        //map.put(AutoDateHistogramAggregationBuilder.NAME, (p, c) -> ParsedAutoDateHistogram.fromXContent(p, (String) c));
        map.put(StringTerms.NAME, (p, c) -> ParsedStringTerms.fromXContent(p, (String) c));
        map.put(LongTerms.NAME, (p, c) -> ParsedLongTerms.fromXContent(p, (String) c));
        map.put(DoubleTerms.NAME, (p, c) -> ParsedDoubleTerms.fromXContent(p, (String) c));
        map.put(MissingAggregationBuilder.NAME, (p, c) -> ParsedMissing.fromXContent(p, (String) c));
        map.put(NestedAggregationBuilder.NAME, (p, c) -> ParsedNested.fromXContent(p, (String) c));
        map.put(ReverseNestedAggregationBuilder.NAME, (p, c) -> ParsedReverseNested.fromXContent(p, (String) c));
        map.put(GlobalAggregationBuilder.NAME, (p, c) -> ParsedGlobal.fromXContent(p, (String) c));
        map.put(FilterAggregationBuilder.NAME, (p, c) -> ParsedFilter.fromXContent(p, (String) c));
        map.put(InternalSampler.PARSER_NAME, (p, c) -> ParsedSampler.fromXContent(p, (String) c));
        map.put(RangeAggregationBuilder.NAME, (p, c) -> ParsedRange.fromXContent(p, (String) c));
        map.put(DateRangeAggregationBuilder.NAME, (p, c) -> ParsedDateRange.fromXContent(p, (String) c));
        map.put(GeoDistanceAggregationBuilder.NAME, (p, c) -> ParsedGeoDistance.fromXContent(p, (String) c));
        map.put(FiltersAggregationBuilder.NAME, (p, c) -> ParsedFilters.fromXContent(p, (String) c));
        map.put(AdjacencyMatrixAggregationBuilder.NAME, (p, c) -> ParsedAdjacencyMatrix.fromXContent(p, (String) c));
        map.put(SignificantLongTerms.NAME, (p, c) -> ParsedSignificantLongTerms.fromXContent(p, (String) c));
        map.put(SignificantStringTerms.NAME, (p, c) -> ParsedSignificantStringTerms.fromXContent(p, (String) c));
        map.put(ScriptedMetricAggregationBuilder.NAME, (p, c) -> ParsedScriptedMetric.fromXContent(p, (String) c));
        map.put(IpRangeAggregationBuilder.NAME, (p, c) -> ParsedBinaryRange.fromXContent(p, (String) c));
        map.put(TopHitsAggregationBuilder.NAME, (p, c) -> ParsedTopHits.fromXContent(p, (String) c));
        map.put(CompositeAggregationBuilder.NAME, (p, c) -> ParsedComposite.fromXContent(p, (String) c));
        final List<NamedXContentRegistry.Entry> entries =
                map.entrySet().stream()
                        .map(entry -> new NamedXContentRegistry.Entry(Aggregation.class, new ParseField(entry.getKey()), entry.getValue()))
                        .collect(Collectors.toList());
        //        entries.add(new NamedXContentRegistry.Entry(Suggest.Suggestion.class, new ParseField(TermSuggestion.NAME),
        //                (parser, context) -> TermSuggestion.fromXContent(parser, (String) context)));
        //        entries.add(new NamedXContentRegistry.Entry(Suggest.Suggestion.class, new ParseField(PhraseSuggestion.NAME),
        //                (parser, context) -> PhraseSuggestion.fromXContent(parser, (String) context)));
        //        entries.add(new NamedXContentRegistry.Entry(Suggest.Suggestion.class, new ParseField(CompletionSuggestion.NAME),
        //                (parser, context) -> CompletionSuggestion.fromXContent(parser, (String) context)));
        return entries;
    }

    protected List<NamedXContentRegistry.Entry> getProvidedNamedXContents() {
        final List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        for (final NamedXContentProvider service : ServiceLoader.load(NamedXContentProvider.class)) {
            entries.addAll(service.getNamedXContentParsers());
        }
        return entries;
    }

    public NamedXContentRegistry getNamedXContentRegistry() {
        return namedXContentRegistry;
    }

    public void addRequestBuilder(final Function<CurlRequest, CurlRequest> builder) {
        requestBuilderList.add(builder);
    }

    protected static class WorkerThread extends ForkJoinWorkerThread {
        protected WorkerThread(ForkJoinPool pool) {
            super(pool);
            setName("eshttp");
        }
    }
}
