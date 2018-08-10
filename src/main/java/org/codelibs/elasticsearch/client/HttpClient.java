package org.codelibs.elasticsearch.client;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.rest.action.RestActions.FAILED_FIELD;
import static org.elasticsearch.rest.action.RestActions.FAILURES_FIELD;
import static org.elasticsearch.rest.action.RestActions.SUCCESSFUL_FIELD;
import static org.elasticsearch.rest.action.RestActions.TOTAL_FIELD;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.lucene.search.Explanation;
import org.codelibs.curl.Curl;
import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.io.stream.ByteArrayStreamOutput;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksAction;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksRequest;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistAction;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
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
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeResponse;
import org.elasticsearch.action.admin.indices.shrink.ShrinkAction;
import org.elasticsearch.action.admin.indices.validate.query.QueryExplanation;
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
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
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
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.PendingClusterTask;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.flush.ShardsSyncedFlushResult;
import org.elasticsearch.indices.flush.SyncedFlushService;
import org.elasticsearch.threadpool.ThreadPool;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

public class HttpClient extends AbstractClient {

    protected static final Logger logger = Logger.getLogger(HttpClient.class.getName());

    protected static final ParseField SHARD_FIELD = new ParseField("shard");

    protected static final ParseField INDEX_FIELD = new ParseField("index");

    protected static final ParseField QUERY_FIELD = new ParseField("query");

    protected static final ParseField STATUS_FIELD = new ParseField("status");

    protected static final ParseField REASON_FIELD = new ParseField("reason");

    protected static final ParseField ACKNOWLEDGED_FIELD = new ParseField("acknowledged");

    protected static final ParseField ALIASES_FIELD = new ParseField("aliases");

    protected static final ParseField MAPPINGS_FIELD = new ParseField("mappings");

    protected static final ParseField FIELDS_FIELD = new ParseField("fields");

    protected static final ParseField SETTINGS_FIELD = new ParseField("settings");

    protected static final ParseField TYPE_FIELD = new ParseField("type");

    protected static final ParseField SEARCHABLE_FIELD = new ParseField("searchable");

    protected static final ParseField AGGREGATABLE_FIELD = new ParseField("aggregatable");

    protected static final ParseField INDICES_FIELD = new ParseField("indices");

    protected static final ParseField NON_SEARCHABLE_INDICES_FIELD = new ParseField("non_searchable_indices");

    protected static final ParseField NON_AGGREGATABLE_INDICES_FIELD = new ParseField("non_aggregatable_indices");

    protected static final ParseField _INDEX_FIELD = new ParseField("_index");

    protected static final ParseField _TYPE_FIELD = new ParseField("_type");

    protected static final ParseField _ID_FIELD = new ParseField("_id");

    protected static final ParseField _ROUTING_FIELD = new ParseField("_routing");

    protected static final ParseField _VERSION_FIELD = new ParseField("_version");

    protected static final ParseField EXPLANATION_FIELD = new ParseField("explanation");

    protected static final ParseField VALUE_FIELD = new ParseField("value");

    protected static final ParseField DESCRIPTION_FIELD = new ParseField("description");

    protected static final ParseField DETAILS_FIELD = new ParseField("details");

    protected static final ParseField CLUSTER_NAME_FIELD = new ParseField("cluster_name");

    protected static final ParseField TIMED_OUT_FIELD = new ParseField("timed_out");

    protected static final ParseField NUMBER_OF_NODES_FIELD = new ParseField("number_of_nodes");

    protected static final ParseField NUMBER_OF_DATA_NODES_FIELD = new ParseField("number_of_data_nodes");

    protected static final ParseField NUMBER_OF_PENDING_TASKS_FIELD = new ParseField("number_of_pending_tasks");

    protected static final ParseField NUMBER_OF_IN_FLIGHT_FETCH_FIELD = new ParseField("number_of_in_flight_fetch");

    protected static final ParseField DELAYED_UNASSIGNED_SHARDS_FIELD = new ParseField("delayed_unassigned_shards");

    protected static final ParseField TASK_MAX_WAIT_TIME_IN_QUEUE_FIELD = new ParseField("task_max_waiting_in_queue");

    protected static final ParseField TASK_MAX_WAIT_TIME_IN_QUEUE_IN_MILLIS_FIELD = new ParseField("task_max_waiting_in_queue_millis");

    protected static final ParseField ACTIVE_SHARDS_PERCENT_AS_NUMBER_FIELD = new ParseField("active_shards_percent_as_number");

    protected static final ParseField ACTIVE_SHARDS_PERCENT_FIELD = new ParseField("active_shards_percent");

    protected static final ParseField ACTIVE_PRIMARY_SHARDS_FIELD = new ParseField("active_primary_shards");

    protected static final ParseField ACTIVE_SHARDS_FIELD = new ParseField("active_shards");

    protected static final ParseField RELOCATING_SHARDS_FIELD = new ParseField("relocating_shards");

    protected static final ParseField INITIALIZING_SHARDS_FIELD = new ParseField("initializing_shards");

    protected static final ParseField UNASSIGNED_SHARDS_FIELD = new ParseField("unassigned_shards");

    protected static final ParseField EXPLANATIONS_FIELD = new ParseField("explanations");

    protected static final ParseField VALID_FIELD = new ParseField("valid");

    protected static final ParseField _SHARDS_FIELD = new ParseField("_shards");

    protected static final ParseField ERROR_FIELD = new ParseField("error");

    protected static final ParseField TASKS_FIELD = new ParseField("tasks");

    protected static final ParseField INSERT_ORDER_FIELD = new ParseField("insert_order");

    protected static final ParseField PRIORITY_FIELD = new ParseField("priority");

    protected static final ParseField SOURCE_FIELD = new ParseField("source");

    protected static final ParseField TIME_IN_QUEUE_MILLIS_FIELD = new ParseField("time_in_queue_millis");

    protected static final ParseField TIME_IN_QUEUE_FIELD = new ParseField("time_in_queue");

    protected static final ParseField EXECUTING_FIELD = new ParseField("executing");

    protected static final ParseField GET_FIELD = new ParseField("get");

    protected static final ParseField TOTAL_FIELD = new ParseField("total");

    protected static final ParseField SUCCESSFUL_FIELD = new ParseField("successful");

    protected static final ParseField FAILED_FIELD = new ParseField("failed");

    protected static final ParseField FAILURES_FIELD = new ParseField("failures");

    protected static final ParseField STATE_FIELD = new ParseField("state");

    protected static final ParseField PRIMARY_FIELD = new ParseField("primary");

    protected static final ParseField NODE_FIELD = new ParseField("node");

    protected static final ParseField RELOCATING_NODE_FIELD = new ParseField("relocating_node");

    protected static final ParseField EXPECTED_SHARD_SIZE_IN_BYTES_FIELD = new ParseField("expected_shard_size_in_bytes");

    protected static final ParseField ROUTING_FIELD = new ParseField("routing");

    protected static final ParseField FULL_NAME_FIELD = new ParseField("full_name");

    protected static final ParseField MAPPING_FIELD = new ParseField("mapping");

    protected static final ParseField UNASSIGNED_INFO_FIELD = new ParseField("unassigned_info");

    protected static final ParseField ALLOCATION_ID_FIELD = new ParseField("allocation_id");

    protected static final ParseField RECOVERY_SOURCE_FIELD = new ParseField("recovery_source");

    protected static final ParseField AT_FIELD = new ParseField("at");

    protected static final ParseField FAILED_ATTEMPTS_FIELD = new ParseField("failed_attempts");

    protected static final ParseField ALLOCATION_STATUS_FIELD = new ParseField("allocation_status");

    protected static final ParseField DELAYED_FIELD = new ParseField("delayed");

    protected static final Function<String, CurlRequest> GET = Curl::get;

    protected static final Function<String, CurlRequest> POST = Curl::post;

    protected static final Function<String, CurlRequest> PUT = Curl::put;

    protected static final Function<String, CurlRequest> DELETE = Curl::delete;

    protected static final Function<String, CurlRequest> HEAD = Curl::head;

    private String[] hosts;

    protected enum ContentType {
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
        } else if (IndexAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.index.IndexAction
            @SuppressWarnings("unchecked")
            final ActionListener<IndexResponse> actionListener = (ActionListener<IndexResponse>) listener;
            processIndexAction((IndexAction) action, (IndexRequest) request, actionListener);
        } else if (FieldCapabilitiesAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.fieldcaps.FieldCapabilitiesAction)
            @SuppressWarnings("unchecked")
            final ActionListener<FieldCapabilitiesResponse> actionListener = (ActionListener<FieldCapabilitiesResponse>) listener;
            processFieldCapabilitiesAction((FieldCapabilitiesAction) action, (FieldCapabilitiesRequest) request, actionListener);
        } else if (GetAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.get.GetAction
            @SuppressWarnings("unchecked")
            final ActionListener<GetResponse> actionListener = (ActionListener<GetResponse>) listener;
            processGetAction((GetAction) action, (GetRequest) request, actionListener);
        } else if (MultiGetAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.get.MultiGetAction
            @SuppressWarnings("unchecked")
            final ActionListener<MultiGetResponse> actionListener = (ActionListener<MultiGetResponse>) listener;
            processMultiGetAction((MultiGetAction) action, (MultiGetRequest) request, actionListener);
        } else if (UpdateAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.update.UpdateAction
            @SuppressWarnings("unchecked")
            final ActionListener<UpdateResponse> actionListener = (ActionListener<UpdateResponse>) listener;
            processUpdateAction((UpdateAction) action, (UpdateRequest) request, actionListener);
        } else if (BulkAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.bulk.BulkAction
            @SuppressWarnings("unchecked")
            final ActionListener<BulkResponse> actionListener = (ActionListener<BulkResponse>) listener;
            processBulkAction((BulkAction) action, (BulkRequest) request, actionListener);
        } else if (DeleteAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.delete.DeleteAction
            @SuppressWarnings("unchecked")
            final ActionListener<DeleteResponse> actionListener = (ActionListener<DeleteResponse>) listener;
            processDeleteAction((DeleteAction) action, (DeleteRequest) request, actionListener);
        } else if (ExplainAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.explain.ExplainAction
            @SuppressWarnings("unchecked")
            final ActionListener<ExplainResponse> actionListener = (ActionListener<ExplainResponse>) listener;
            processExplainAction((ExplainAction) action, (ExplainRequest) request, actionListener);
        } else if (UpdateSettingsAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction
            @SuppressWarnings("unchecked")
            final ActionListener<UpdateSettingsResponse> actionListener = (ActionListener<UpdateSettingsResponse>) listener;
            processUpdateSettingsAction((UpdateSettingsAction) action, (UpdateSettingsRequest) request, actionListener);
        } else if (GetSettingsAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction
            @SuppressWarnings("unchecked")
            final ActionListener<GetSettingsResponse> actionListener = (ActionListener<GetSettingsResponse>) listener;
            processGetSettingsAction((GetSettingsAction) action, (GetSettingsRequest) request, actionListener);
        } else if (ForceMergeAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction
            @SuppressWarnings("unchecked")
            final ActionListener<ForceMergeResponse> actionListener = (ActionListener<ForceMergeResponse>) listener;
            processForceMergeAction((ForceMergeAction) action, (ForceMergeRequest) request, actionListener);
        } else if (MainAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.main.MainAction
            @SuppressWarnings("unchecked")
            final ActionListener<MainResponse> actionListener = (ActionListener<MainResponse>) listener;
            processMainAction((MainAction) action, (MainRequest) request, actionListener);
        } else if (ClusterUpdateSettingsAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction
            @SuppressWarnings("unchecked")
            final ActionListener<ClusterUpdateSettingsResponse> actionListener = (ActionListener<ClusterUpdateSettingsResponse>) listener;
            processClusterUpdateSettingsAction((ClusterUpdateSettingsAction) action, (ClusterUpdateSettingsRequest) request, actionListener);
        } else if (ClusterHealthAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.cluster.health.ClusterHealthAction
            @SuppressWarnings("unchecked")
            final ActionListener<ClusterHealthResponse> actionListener = (ActionListener<ClusterHealthResponse>) listener;
            processClusterHealthAction((ClusterHealthAction) action, (ClusterHealthRequest) request, actionListener);
        } else if (AliasesExistAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.alias.exists.AliasesExistAction
            @SuppressWarnings("unchecked")
            final ActionListener<AliasesExistResponse> actionListener = (ActionListener<AliasesExistResponse>) listener;
            processAliasesExistAction((AliasesExistAction) action, (GetAliasesRequest) request, actionListener);
        } else if (ValidateQueryAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction
            @SuppressWarnings("unchecked")
            final ActionListener<ValidateQueryResponse> actionListener = (ActionListener<ValidateQueryResponse>) listener;
            processValidateQueryAction((ValidateQueryAction) action, (ValidateQueryRequest) request, actionListener);
        } else if (PendingClusterTasksAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksAction
            @SuppressWarnings("unchecked")
            final ActionListener<PendingClusterTasksResponse> actionListener = (ActionListener<PendingClusterTasksResponse>) listener;
            processPendingClusterTasksAction((PendingClusterTasksAction) action, (PendingClusterTasksRequest) request, actionListener);
        } else if (GetAliasesAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction
            @SuppressWarnings("unchecked")
            final ActionListener<GetAliasesResponse> actionListener = (ActionListener<GetAliasesResponse>) listener;
            processGetAliasesAction((GetAliasesAction) action, (GetAliasesRequest) request, actionListener);
        } else if (SyncedFlushAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.flush.SyncedFlushAction
            @SuppressWarnings("unchecked")
            final ActionListener<SyncedFlushResponse> actionListener = (ActionListener<SyncedFlushResponse>) listener;
            processSyncedFlushAction((SyncedFlushAction) action, (SyncedFlushRequest) request, actionListener);
        } else if (GetFieldMappingsAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction
            @SuppressWarnings("unchecked")
            final ActionListener<GetFieldMappingsResponse> actionListener = (ActionListener<GetFieldMappingsResponse>) listener;
            processGetFieldMappingsAction((GetFieldMappingsAction) action, (GetFieldMappingsRequest) request, actionListener);
        } else if (ShrinkAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.shrink.ShrinkAction
            @SuppressWarnings("unchecked")
            final ActionListener<ResizeResponse> actionListener = (ActionListener<ResizeResponse>) listener;
            processShrinkAction((ShrinkAction) action, (ResizeRequest) request, actionListener);
        } else if (TypesExistsAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.exists.types.TypesExistsAction
            @SuppressWarnings("unchecked")
            final ActionListener<TypesExistsResponse> actionListener = (ActionListener<TypesExistsResponse>) listener;
            processTypesExistsAction((TypesExistsAction) action, (TypesExistsRequest) request, actionListener);
        } else if (RolloverAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.rollover.RolloverAction
            @SuppressWarnings("unchecked")
            final ActionListener<RolloverResponse> actionListener = (ActionListener<RolloverResponse>) listener;
            processRolloverAction((RolloverAction) action, (RolloverRequest) request, actionListener);
        } else if (ClearIndicesCacheAction.INSTANCE.equals(action)) {
            // org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheAction
            @SuppressWarnings("unchecked")
            final ActionListener<ClearIndicesCacheResponse> actionListener = (ActionListener<ClearIndicesCacheResponse>) listener;
            processClearIndicesCacheAction((ClearIndicesCacheAction) action, (ClearIndicesCacheRequest) request, actionListener);
        } else {

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

            // org.elasticsearch.action.ingest.DeletePipelineAction
            // org.elasticsearch.action.ingest.PutPipelineAction
            // org.elasticsearch.action.ingest.SimulatePipelineAction
            // org.elasticsearch.action.ingest.GetPipelineAction
            // org.elasticsearch.action.termvectors.MultiTermVectorsAction
            // org.elasticsearch.action.termvectors.TermVectorsAction
            // org.elasticsearch.action.admin.indices.shards.IndicesShardStoresAction
            // org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction
            // org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction
            // org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateAction
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
            // org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptAction
            // org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptAction
            // org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptAction

            throw new UnsupportedOperationException("Action: " + action.name());
        }
    }

    protected void processClearIndicesCacheAction(final ClearIndicesCacheAction action, final ClearIndicesCacheRequest request,
            final ActionListener<ClearIndicesCacheResponse> listener) {
        String fields = null;
        if (request.fields() != null && request.fields().length > 0) {
            fields = String.join(",", request.fields());
        }
        getCurlRequest(POST, "/_cache/clear", request.indices()).param("fielddata", String.valueOf(request.fieldDataCache()))
                .param("query", String.valueOf(request.queryCache())).param("request", String.valueOf(request.requestCache()))
                .param("fields", fields).execute(response -> {
                    if (response.getHttpStatusCode() != 200) {
                        throw new ElasticsearchException("error: " + response.getHttpStatusCode());
                    }
                    try (final InputStream in = response.getContentAsStream()) {
                        final XContentParser parser = createParser(in);
                        final ClearIndicesCacheResponse clearIndicesCacheResponse = ClearIndicesCacheResponse.fromXContent(parser);
                        listener.onResponse(clearIndicesCacheResponse);
                    } catch (final Exception e) {
                        listener.onFailure(e);
                    }
                }, listener::onFailure);
    }

    protected void processRolloverAction(final RolloverAction action, final RolloverRequest request,
            final ActionListener<RolloverResponse> listener) {
        String source = null;
        try {
            final XContentBuilder builder = request.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS);
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to parse a request.", e);
        }
        getCurlRequest(POST, "/_rollover" + (request.getNewIndexName() != null ? "/" + request.getNewIndexName() : ""), request.getAlias())
                .param("dry_run", (request.isDryRun() ? "" : null)).body(source).execute(response -> {
                    if (response.getHttpStatusCode() != 200) {
                        throw new ElasticsearchException("error: " + response.getHttpStatusCode());
                    }
                    try (final InputStream in = response.getContentAsStream()) {
                        final XContentParser parser = createParser(in);
                        final RolloverResponse rolloverResponse = RolloverResponse.fromXContent(parser);
                        listener.onResponse(rolloverResponse);
                    } catch (final Exception e) {
                        listener.onFailure(e);
                    }
                }, listener::onFailure);
    }

    protected void processTypesExistsAction(final TypesExistsAction action, final TypesExistsRequest request,
            final ActionListener<TypesExistsResponse> listener) {
        getCurlRequest(HEAD, "/_mapping/" + String.join(",", request.types()), request.indices()).execute(response -> {
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
                final TypesExistsResponse typesExistsResponse = new TypesExistsResponse(exists);
                listener.onResponse(typesExistsResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }

    protected void processShrinkAction(final ShrinkAction action, final ResizeRequest request, final ActionListener<ResizeResponse> listener) {
        getCurlRequest(POST, "/_shrink/" + request.getTargetIndexRequest().index(), request.getSourceIndex()).execute(response -> {
            if (response.getHttpStatusCode() != 200) {
                throw new ElasticsearchException("error: " + response.getHttpStatusCode());
            }
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final ResizeResponse resizeResponse = ResizeResponse.fromXContent(parser);
                listener.onResponse(resizeResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }

    protected void processGetFieldMappingsAction(final GetFieldMappingsAction action, final GetFieldMappingsRequest request,
            final ActionListener<GetFieldMappingsResponse> listener) {
        final StringBuilder pathSuffix = new StringBuilder(100);
        if (request.types().length > 0) {
            pathSuffix.append(String.join(",", request.types())).append('/');
        }
        pathSuffix.append("field/");
        if (request.fields().length > 0) {
            pathSuffix.append(String.join(",", request.fields()));
        }

        getCurlRequest(GET, "/_mapping/" + pathSuffix.toString(), request.indices()).param("include_defaults",
                String.valueOf(request.includeDefaults())).execute(
                response -> {
                    if (response.getHttpStatusCode() != 200) {
                        throw new ElasticsearchException("Indices are not found: " + response.getHttpStatusCode());
                    }
                    try (final InputStream in = response.getContentAsStream()) {
                        final XContentParser parser = createParser(in);
                        final GetFieldMappingsResponse getFieldMappingsResponse =
                                getGetFieldMappingsResponsefromXContent(parser, action::newResponse);
                        listener.onResponse(getFieldMappingsResponse);
                    } catch (final Exception e) {
                        listener.onFailure(e);
                    }
                }, listener::onFailure);
    }

    protected GetFieldMappingsResponse getGetFieldMappingsResponsefromXContent(final XContentParser parser,
            final Supplier<GetFieldMappingsResponse> newResponse) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);

        final Map<String, Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetaData>>> mappings = new HashMap<>();
        if (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
            while (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                final String index = parser.currentName();

                final Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetaData>> typeMappings =
                        parseTypeMappings(parser, index);
                mappings.put(index, typeMappings);

                parser.nextToken();
            }
        }

        return newGetFieldMappingsResponse(mappings);
    }

    protected Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetaData>> parseTypeMappings(final XContentParser parser,
            final String index) throws IOException {
        final ObjectParser<Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetaData>>, String> objectParser =
                new ObjectParser<>(MAPPINGS_FIELD.getPreferredName(), true, HashMap::new);

        objectParser.declareField((p, typeMappings, idx) -> {
            p.nextToken();
            while (p.currentToken() == XContentParser.Token.FIELD_NAME) {
                final String typeName = p.currentName();

                if (p.nextToken() == XContentParser.Token.START_OBJECT) {
                    final Map<String, GetFieldMappingsResponse.FieldMappingMetaData> typeMapping = new HashMap<>();
                    typeMappings.put(typeName, typeMapping);

                    while (p.nextToken() == XContentParser.Token.FIELD_NAME) {
                        final String fieldName = p.currentName();
                        final GetFieldMappingsResponse.FieldMappingMetaData fieldMappingMetaData = getFieldMappingMetaDatafromXContent(p);
                        typeMapping.put(fieldName, fieldMappingMetaData);
                    }
                } else {
                    p.skipChildren();
                }
                p.nextToken();
            }
        }, MAPPINGS_FIELD, ObjectParser.ValueType.OBJECT);
        return objectParser.parse(parser, index);
    }

    protected GetFieldMappingsResponse.FieldMappingMetaData getFieldMappingMetaDatafromXContent(final XContentParser parser)
            throws IOException {
        final ConstructingObjectParser<GetFieldMappingsResponse.FieldMappingMetaData, String> objectParser =
                new ConstructingObjectParser<>("field_mapping_meta_data", true, a -> new GetFieldMappingsResponse.FieldMappingMetaData(
                        (String) a[0], (BytesReference) a[1]));
        objectParser.declareField(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.text(), FULL_NAME_FIELD,
                ObjectParser.ValueType.STRING);
        objectParser.declareField(ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> BytesReference.bytes(XContentFactory.jsonBuilder().copyCurrentStructure(p)), MAPPING_FIELD,
                ObjectParser.ValueType.OBJECT);
        return objectParser.parse(parser, null);
    }

    protected GetFieldMappingsResponse newGetFieldMappingsResponse(
            Map<String, Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetaData>>> mappings) {
        final Class<GetFieldMappingsResponse> clazz = GetFieldMappingsResponse.class;
        final Class<?>[] types = { Map.class };
        try {
            final Constructor<GetFieldMappingsResponse> constructor = clazz.getDeclaredConstructor(types);
            constructor.setAccessible(true);
            return constructor.newInstance(mappings);
        } catch (final Exception e) {
            throw new ElasticsearchException("Failed to create GetFieldMappingsResponse.", e);
        }
    }

    // "hard to reconstruct the whole response from info via REST" (via https://github.com/elastic/elasticsearch/issues/27205)
    protected void processSyncedFlushAction(final SyncedFlushAction action, final SyncedFlushRequest request,
            final ActionListener<SyncedFlushResponse> listener) {
        getCurlRequest(POST, "/_flush/synced", request.indices()).execute(response -> {
            if (response.getHttpStatusCode() != 200) {
                throw new ElasticsearchException("Indices are not found: " + response.getHttpStatusCode());
            }
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final SyncedFlushResponse syncedFlushResponse = getSyncedFlushResponsefromXContent(parser, action::newResponse);
                listener.onResponse(syncedFlushResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }

    protected SyncedFlushResponse getSyncedFlushResponsefromXContent(final XContentParser parser,
            final Supplier<SyncedFlushResponse> newResponse) throws IOException {
        //  Fields for ShardCounts
        int totalShards = 0;
        int successfulShards = 0;
        int failedShards = 0;

        final Map<String, List<ShardsSyncedFlushResult>> shardsResultPerIndex = new HashMap<>();

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
        XContentParser.Token token;
        String index = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == Token.FIELD_NAME) {
                index = parser.currentName();
            } else if (token == Token.START_OBJECT) {
                if (_SHARDS_FIELD.match(index, LoggingDeprecationHandler.INSTANCE)) {
                    String currentFieldName = null;
                    while ((token = parser.nextToken()) != Token.END_OBJECT) {
                        if (token == Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token.isValue()) {
                            if (TOTAL_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                                totalShards = parser.intValue();
                            } else if (SUCCESSFUL_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                                successfulShards = parser.intValue();
                            } else if (FAILED_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                                failedShards = parser.intValue();
                            } else {
                                parser.skipChildren();
                            }
                        }
                    }
                } else {
                    final String uuid = ""; // UUID of "index"
                    final Index idx = new Index(index, uuid);
                    shardsResultPerIndex.put(index, parseShardsSyncedFlushResults(parser, idx));
                }
            }
        }

        try (final ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
            out.writeInt(totalShards);
            out.writeInt(successfulShards);
            out.writeInt(failedShards);
            out.writeInt(shardsResultPerIndex.size());
            for (Map.Entry<String, List<ShardsSyncedFlushResult>> entry : shardsResultPerIndex.entrySet()) {
                out.writeString(entry.getKey());
                out.writeInt(entry.getValue().size());
                for (ShardsSyncedFlushResult shardsSyncedFlushResult : entry.getValue()) {
                    shardsSyncedFlushResult.writeTo(out);
                }
            }
            final SyncedFlushResponse response = newResponse.get();
            response.readFrom(out.toStreamInput());
            return response;
        }
    }

    protected List<ShardsSyncedFlushResult> parseShardsSyncedFlushResults(final XContentParser parser, final Index index)
            throws IOException {
        // "failures" fields
        final List<ShardsSyncedFlushResult> shardsSyncedFlushResults = new ArrayList<>();
        int total = 0;
        int successful = 0;
        int failed = 0;
        XContentParser.Token token;
        String currentFieldName = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == Token.START_ARRAY) {
                if (FAILURES_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);
                        shardsSyncedFlushResults.add(parseShardFailuresResults(parser, index, total));
                    }
                } else {
                    parser.skipChildren();
                }
            } else if (token.isValue()) {
                if (TOTAL_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    total = parser.intValue();
                } else if (SUCCESSFUL_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    successful = parser.intValue();
                } else if (FAILED_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    failed = parser.intValue();
                } else {
                    parser.skipChildren();
                }
            }
        }

        return shardsSyncedFlushResults;
    }

    protected ShardsSyncedFlushResult parseShardFailuresResults(final XContentParser parser, final Index index, final int totalShards)
            throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);

        String failureReason = null;
        int shardIdValue = 0;
        final Map<ShardRouting, SyncedFlushService.ShardSyncedFlushResponse> shardResponses = new HashMap<>();
        String currentFieldName = null;
        for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
            if (token == Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == Token.START_OBJECT) {
                if (ROUTING_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    shardResponses.put(parseShardRouting(parser), new SyncedFlushService.ShardSyncedFlushResponse(failureReason));
                } else {
                    parser.skipChildren();
                }
            } else if (token.isValue()) {
                if (SHARD_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    shardIdValue = parser.intValue();
                } else if (REASON_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    failureReason = parser.text();
                } else {
                    parser.skipChildren();
                }
            }
        }

        if (shardResponses.isEmpty()) {
            return new ShardsSyncedFlushResult(new ShardId(index, shardIdValue), totalShards, failureReason);
        } else {
            final String syncId = ""; // TODO
            return new ShardsSyncedFlushResult(new ShardId(index, shardIdValue), syncId, totalShards, shardResponses);
        }
    }

    protected ShardRouting parseShardRouting(final XContentParser parser) throws IOException {
        @SuppressWarnings("unchecked")
        final ConstructingObjectParser<ShardRouting, Void> objectParser = new ConstructingObjectParser<>("routing", true, a -> {
            try (final ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
                int i = 0;

                final ShardRoutingState state = ShardRoutingState.valueOf((String) a[i++]);
                final boolean primary = (boolean) a[i++];
                final String currentNodeId = (String) a[i++];
                final String relocatingNodeId = (String) a[i++];
                final int shardIdValue = (int) a[i++];
                final String index = (String) a[i++];
                final long expectedShardSize = (long) a[i++];
                final String uuid = ""; // TODO
                final ShardId shardId = new ShardId(new Index(index, uuid), shardIdValue);
                final UnassignedInfo unassignedInfo = (UnassignedInfo) a[i++];
                final AllocationId allocationId = (AllocationId) a[i++];
                final RecoverySource recoverySource = (RecoverySource) a[i++];

                out.writeOptionalString(currentNodeId);
                out.writeOptionalString(relocatingNodeId);
                out.writeBoolean(primary);
                out.writeByte(state.value());
                if (state == ShardRoutingState.UNASSIGNED || state == ShardRoutingState.INITIALIZING) {
                    recoverySource.writeTo(out);
                }
                out.writeOptionalWriteable(unassignedInfo);
                out.writeOptionalWriteable(allocationId);
                if (state == ShardRoutingState.RELOCATING || state == ShardRoutingState.INITIALIZING) {
                    out.writeLong(expectedShardSize);
                }

                return new ShardRouting(shardId, out.toStreamInput());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        objectParser.declareString(ConstructingObjectParser.constructorArg(), STATE_FIELD);
        objectParser.declareBoolean(ConstructingObjectParser.constructorArg(), PRIMARY_FIELD);
        objectParser.declareString(ConstructingObjectParser.constructorArg(), NODE_FIELD);
        objectParser.declareString(ConstructingObjectParser.constructorArg(), RELOCATING_NODE_FIELD);
        objectParser.declareInt(ConstructingObjectParser.constructorArg(), SHARD_FIELD);
        objectParser.declareString(ConstructingObjectParser.constructorArg(), INDEX_FIELD);
        objectParser.declareLong(ConstructingObjectParser.constructorArg(), EXPECTED_SHARD_SIZE_IN_BYTES_FIELD);
        objectParser.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
            try {
                return getUnassignedInfo(p);
            } catch (Exception e) {
                throw new ElasticsearchException("Failed to create SyncedFlushResponse.", e);
            }
        }, UNASSIGNED_INFO_FIELD);
        objectParser.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> AllocationId.fromXContent(p),
                ALLOCATION_ID_FIELD);
        objectParser
                .declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> getRecoverySource(p), RECOVERY_SOURCE_FIELD);

        return objectParser.apply(parser, null);
    }

    protected UnassignedInfo getUnassignedInfo(final XContentParser parser) throws Exception {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);

        UnassignedInfo.Reason reason = null;
        long unassignedTimeMillis = 0;
        int failedAllocations = 0;
        boolean delayed = false;
        String details = null;
        UnassignedInfo.AllocationStatus allocationStatus = null;
        String currentFieldName = null;
        for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
            if (token == Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (REASON_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    reason = UnassignedInfo.Reason.values()[parser.intValue()];
                } else if (AT_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    final SimpleDateFormat formatter = new SimpleDateFormat(Joda.forPattern("dateOptionalTime").format());
                    unassignedTimeMillis = formatter.parse(parser.text()).getTime();
                } else if (FAILED_ATTEMPTS_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    failedAllocations = parser.intValue();
                } else if (DELAYED_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    delayed = parser.booleanValue();
                } else if (DETAILS_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    details = parser.text();
                } else if (ALLOCATION_STATUS_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    allocationStatus = UnassignedInfo.AllocationStatus.values()[parser.intValue()];
                } else {
                    parser.skipChildren();
                }
            }
        }

        return new UnassignedInfo(reason, null, null, failedAllocations, unassignedTimeMillis * 1000000L, unassignedTimeMillis, delayed,
                allocationStatus);
    }

    protected RecoverySource getRecoverySource(final XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);

        byte type = -1;
        String currentFieldName = null;
        for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
            if (token == Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (TYPE_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    type = (byte) parser.intValue();
                } else {
                    parser.skipChildren();
                }
            }
        }

        try (final ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
            out.writeByte(type);
            return RecoverySource.readFrom(out.toStreamInput());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected void processGetAliasesAction(final GetAliasesAction action, final GetAliasesRequest request,
            final ActionListener<GetAliasesResponse> listener) {
        getCurlRequest(GET, "/_alias/" + String.join(",", request.aliases()), request.indices()).execute(response -> {
            if (response.getHttpStatusCode() != 200) {
                throw new ElasticsearchException("Indices are not found: " + response.getHttpStatusCode());
            }
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final GetAliasesResponse getAliasesResponse = getGetAliasesResponsefromXContent(parser, action::newResponse);
                listener.onResponse(getAliasesResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }

    protected GetAliasesResponse getGetAliasesResponsefromXContent(final XContentParser parser,
            final Supplier<GetAliasesResponse> newResponse) throws IOException {
        @SuppressWarnings("unchecked")
        final ImmutableOpenMap.Builder<String, List<AliasMetaData>> aliasesMapBuilder = ImmutableOpenMap.builder();

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
        XContentParser.Token token;
        String index = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == Token.FIELD_NAME) {
                index = parser.currentName();
            } else if (token == Token.START_OBJECT) {
                while (parser.nextToken() == Token.FIELD_NAME) {
                    final String currentFieldName = parser.currentName();
                    if (ALIASES_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                        aliasesMapBuilder.put(index, getAliasesFromXContent(parser));
                    } else {
                        parser.skipChildren();
                    }
                }
            }
        }

        final ImmutableOpenMap<String, List<AliasMetaData>> aliases = aliasesMapBuilder.build();

        try (final ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
            out.writeVInt(aliases.size());
            for (ObjectObjectCursor<String, List<AliasMetaData>> entry : aliases) {
                out.writeString(entry.key);
                out.writeVInt(entry.value.size());
                for (AliasMetaData aliasMetaData : entry.value) {
                    aliasMetaData.writeTo(out);
                }
            }
            final GetAliasesResponse response = newResponse.get();
            response.readFrom(out.toStreamInput());
            return response;
        }
    }

    protected void processPendingClusterTasksAction(final PendingClusterTasksAction action, final PendingClusterTasksRequest request,
            final ActionListener<PendingClusterTasksResponse> listener) {
        getCurlRequest(GET, "/_cluster/pending_tasks").execute(
                response -> {
                    if (response.getHttpStatusCode() != 200) {
                        throw new ElasticsearchException("Indices are not found: " + response.getHttpStatusCode());
                    }
                    try (final InputStream in = response.getContentAsStream()) {
                        final XContentParser parser = createParser(in);
                        final PendingClusterTasksResponse pendingClusterTasksResponse =
                                getPendingClusterTasksResponsefromXContent(parser, action::newResponse);
                        listener.onResponse(pendingClusterTasksResponse);
                    } catch (final Exception e) {
                        listener.onFailure(e);
                    }
                }, listener::onFailure);
    }

    protected PendingClusterTasksResponse getPendingClusterTasksResponsefromXContent(final XContentParser parser,
            final Supplier<PendingClusterTasksResponse> newResponse) throws IOException {
        @SuppressWarnings("unchecked")
        final ConstructingObjectParser<PendingClusterTasksResponse, Void> objectParser =
                new ConstructingObjectParser<>("pending_cluster_tasks", true, a -> {
                    try (final ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
                        final List<PendingClusterTask> pendingClusterTasks = (a[0] != null ? (List<PendingClusterTask>) a[0] : null);

                        out.writeVInt(pendingClusterTasks.size());
                        for (PendingClusterTask task : pendingClusterTasks) {
                            task.writeTo(out);
                        }

                        final PendingClusterTasksResponse response = newResponse.get();
                        response.readFrom(out.toStreamInput());
                        return response;
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });

        objectParser.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(), getPendingClusterTaskParser(), TASKS_FIELD);
        return objectParser.apply(parser, null);
    }

    protected ConstructingObjectParser getPendingClusterTaskParser() {
        @SuppressWarnings("unchecked")
        final ConstructingObjectParser<PendingClusterTask, Void> objectParser =
                new ConstructingObjectParser<>("tasks", true, a -> new PendingClusterTask((long) a[0], Priority.valueOf((String) a[1]),
                        new Text((String) a[2]), (long) a[3], (a[4] != null ? (Boolean) a[4] : false)));

        objectParser.declareLong(ConstructingObjectParser.constructorArg(), INSERT_ORDER_FIELD);
        objectParser.declareString(ConstructingObjectParser.constructorArg(), PRIORITY_FIELD);
        objectParser.declareString(ConstructingObjectParser.constructorArg(), SOURCE_FIELD);
        objectParser.declareLong(ConstructingObjectParser.constructorArg(), TIME_IN_QUEUE_MILLIS_FIELD);
        objectParser.declareBoolean(ConstructingObjectParser.constructorArg(), EXECUTING_FIELD);
        return objectParser;
    }

    protected void processValidateQueryAction(final ValidateQueryAction action, final ValidateQueryRequest request,
            final ActionListener<ValidateQueryResponse> listener) {
        String source = null;
        try {
            final XContentBuilder builder =
                    XContentFactory.jsonBuilder().startObject().field(QUERY_FIELD.getPreferredName(), request.query()).endObject();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to parse a request.", e);
        }

        getCurlRequest(GET, (request.types() == null ? "" : "/" + String.join(",", request.types())) + "/_validate/query",
                request.indices())
                .param("explain", String.valueOf(request.explain()))
                .param("rewrite", String.valueOf(request.rewrite()))
                .param("all_shards", String.valueOf(request.allShards()))
                .body(source)
                .execute(
                        response -> {
                            if (response.getHttpStatusCode() != 200) {
                                throw new ElasticsearchException("Indices are not found: " + response.getHttpStatusCode());
                            }
                            try (final InputStream in = response.getContentAsStream()) {
                                final XContentParser parser = createParser(in);
                                final ValidateQueryResponse validateQueryResponse =
                                        getValidateQueryResponsefromXContent(parser, action::newResponse);
                                listener.onResponse(validateQueryResponse);
                            } catch (final Exception e) {
                                listener.onFailure(e);
                            }
                        }, listener::onFailure);
    }

    protected ValidateQueryResponse getValidateQueryResponsefromXContent(final XContentParser parser,
            final Supplier<ValidateQueryResponse> newResponse) throws IOException {
        @SuppressWarnings("unchecked")
        final ConstructingObjectParser<ValidateQueryResponse, Void> objectParser =
                new ConstructingObjectParser<>("validate_query", true, a -> {
                    try (final ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
                        BroadcastResponse broadcastResponse = (a[0] != null ? (BroadcastResponse) a[0] : new BroadcastResponse());
                        final boolean valid = (boolean) a[1];
                        final List<QueryExplanation> queryExplanations =
                                (a[2] != null ? (List<QueryExplanation>) a[2] : Collections.emptyList());

                        broadcastResponse.writeTo(out);
                        out.writeBoolean(valid);
                        out.writeVInt(queryExplanations.size());
                        for (QueryExplanation exp : queryExplanations) {
                            exp.writeTo(out);
                        }

                        final ValidateQueryResponse response = newResponse.get();
                        response.readFrom(out.toStreamInput());
                        return response;
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });

        objectParser.declareObject(ConstructingObjectParser.optionalConstructorArg(), getBroadcastParser(), _SHARDS_FIELD);
        objectParser.declareBoolean(ConstructingObjectParser.constructorArg(), VALID_FIELD);
        objectParser.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(), getQueryExplanationParser(), EXPLANATIONS_FIELD);
        return objectParser.apply(parser, null);
    }

    protected ConstructingObjectParser getBroadcastParser() {
        @SuppressWarnings("unchecked")
        final ConstructingObjectParser<BroadcastResponse, Void> shardsParser =
                new ConstructingObjectParser<>("_shards", true, arg -> new BroadcastResponse((int) arg[0], (int) arg[1], (int) arg[2],
                        (List<DefaultShardOperationFailedException>) arg[3]));
        shardsParser.declareInt(ConstructingObjectParser.constructorArg(), TOTAL_FIELD);
        shardsParser.declareInt(ConstructingObjectParser.constructorArg(), SUCCESSFUL_FIELD);
        shardsParser.declareInt(ConstructingObjectParser.constructorArg(), FAILED_FIELD);
        shardsParser.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> DefaultShardOperationFailedException.fromXContent(p), FAILURES_FIELD);
        return shardsParser;
    }

    protected ConstructingObjectParser getQueryExplanationParser() {
        @SuppressWarnings("unchecked")
        final ConstructingObjectParser<QueryExplanation, Void> objectParser =
                new ConstructingObjectParser<>("query_explanation", true, a -> {
                    int shard = QueryExplanation.RANDOM_SHARD;
                    if (a[1] != null) {
                        shard = (int) a[1];
                    }
                    return new QueryExplanation((String) a[0], shard, (boolean) a[2], (String) a[3], (String) a[4]);
                });
        objectParser.declareString(ConstructingObjectParser.optionalConstructorArg(), INDEX_FIELD);
        objectParser.declareInt(ConstructingObjectParser.optionalConstructorArg(), SHARD_FIELD);
        objectParser.declareBoolean(ConstructingObjectParser.constructorArg(), VALID_FIELD);
        objectParser.declareString(ConstructingObjectParser.optionalConstructorArg(), EXPLANATION_FIELD);
        objectParser.declareString(ConstructingObjectParser.optionalConstructorArg(), ERROR_FIELD);
        return objectParser;
    }

    protected void processAliasesExistAction(final AliasesExistAction action, final GetAliasesRequest request,
            final ActionListener<AliasesExistResponse> listener) {
        getCurlRequest(HEAD, "/_alias/" + String.join(",", request.aliases()), request.indices()).execute(response -> {
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
                final AliasesExistResponse aliasesExistResponse = new AliasesExistResponse(exists);
                listener.onResponse(aliasesExistResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }

    protected void processClusterHealthAction(final ClusterHealthAction action, final ClusterHealthRequest request,
            final ActionListener<ClusterHealthResponse> listener) {
        String wait_for_status = null;
        try {
            if (request.waitForStatus() != null) {
                final ClusterHealthStatus clusterHealthStatus = ClusterHealthStatus.fromValue(request.waitForStatus().value());
                wait_for_status = clusterHealthStatus.toString().toLowerCase();
            }
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to parse a request.", e);
        }

        getCurlRequest(GET, "/_cluster/health" + (request.indices() == null ? "" : "/" + String.join(",", request.indices())))
                .param("wait_for_status", wait_for_status)
                .param("wait_for_no_relocating_shards", String.valueOf(request.waitForNoRelocatingShards()))
                .param("wait_for_no_initializing_shards", String.valueOf(request.waitForNoInitializingShards()))
                .param("wait_for_active_shards", (request.waitForActiveShards() == null ? null : request.waitForActiveShards().toString()))
                .param("wait_for_nodes", request.waitForNodes())
                .param("timeout", (request.timeout() == null ? null : request.timeout().toString())).execute(response -> {
                    if (response.getHttpStatusCode() != 200) {
                        throw new ElasticsearchException("Indices are not found: " + response.getHttpStatusCode());
                    }
                    try (final InputStream in = response.getContentAsStream()) {
                        final XContentParser parser = createParser(in);
                        final ClusterHealthResponse clusterHealthResponse = getClusterHealthResponsefromXContent(parser);
                        listener.onResponse(clusterHealthResponse);
                    } catch (final Exception e) {
                        listener.onFailure(e);
                    }
                }, listener::onFailure);
    }

    protected ClusterHealthResponse getClusterHealthResponsefromXContent(final XContentParser parser) throws IOException {
        final ConstructingObjectParser<ClusterHealthResponse, Void> objectParser =
                new ConstructingObjectParser<>("cluster_health_response", true, parsedObjects -> {
                    try (final ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
                        int i = 0;

                        // ClusterStateHealth fields
                        final int numberOfNodes = (int) parsedObjects[i++];
                        final int numberOfDataNodes = (int) parsedObjects[i++];
                        final int activeShards = (int) parsedObjects[i++];
                        final int relocatingShards = (int) parsedObjects[i++];
                        final int activePrimaryShards = (int) parsedObjects[i++];
                        final int initializingShards = (int) parsedObjects[i++];
                        final int unassignedShards = (int) parsedObjects[i++];
                        final double activeShardsPercent = (double) parsedObjects[i++];
                        final String statusStr = (String) parsedObjects[i++];
                        final ClusterHealthStatus clusterHealthStatus = ClusterHealthStatus.fromString(statusStr);

                        @SuppressWarnings("unchecked")
                        // Can be absent if LEVEL == 'cluster'
                        // null because no level option in 6.3
                        final List<ClusterIndexHealth> indexList = null;
                        final int indices_size = (indexList != null && !indexList.isEmpty() ? indexList.size() : 0);

                        // ClusterHealthResponse fields
                        final String clusterName = (String) parsedObjects[i++];
                        final int numberOfPendingTasks = (int) parsedObjects[i++];
                        final int numberOfInFlightFetch = (int) parsedObjects[i++];
                        final int delayedUnassignedShards = (int) parsedObjects[i++];
                        final TimeValue taskMaxWaitingTime = new TimeValue((long) parsedObjects[i++]);
                        final boolean timedOut = (boolean) parsedObjects[i];

                        out.writeString(clusterName);
                        out.writeByte(clusterHealthStatus.value());

                        // write ClusterStateHealth to out
                        out.writeVInt(activePrimaryShards);
                        out.writeVInt(activeShards);
                        out.writeVInt(relocatingShards);
                        out.writeVInt(initializingShards);
                        out.writeVInt(unassignedShards);
                        out.writeVInt(numberOfNodes);
                        out.writeVInt(numberOfDataNodes);
                        out.writeByte(clusterHealthStatus.value());
                        out.writeVInt(indices_size);
                        for (int j = 0; j < indices_size; j++) {
                            indexList.get(i).writeTo(out);
                        }
                        out.writeDouble(activeShardsPercent);

                        out.writeInt(numberOfPendingTasks);
                        out.writeBoolean(timedOut);
                        out.writeInt(numberOfInFlightFetch);
                        out.writeInt(delayedUnassignedShards);
                        out.writeTimeValue(taskMaxWaitingTime);

                        return ClusterHealthResponse.readResponseFrom(out.toStreamInput());
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });

        // ClusterStateHealth fields
        objectParser.declareInt(ConstructingObjectParser.constructorArg(), NUMBER_OF_NODES_FIELD);
        objectParser.declareInt(ConstructingObjectParser.constructorArg(), NUMBER_OF_DATA_NODES_FIELD);
        objectParser.declareInt(ConstructingObjectParser.constructorArg(), ACTIVE_SHARDS_FIELD);
        objectParser.declareInt(ConstructingObjectParser.constructorArg(), RELOCATING_SHARDS_FIELD);
        objectParser.declareInt(ConstructingObjectParser.constructorArg(), ACTIVE_PRIMARY_SHARDS_FIELD);
        objectParser.declareInt(ConstructingObjectParser.constructorArg(), INITIALIZING_SHARDS_FIELD);
        objectParser.declareInt(ConstructingObjectParser.constructorArg(), UNASSIGNED_SHARDS_FIELD);
        objectParser.declareDouble(ConstructingObjectParser.constructorArg(), ACTIVE_SHARDS_PERCENT_AS_NUMBER_FIELD);
        objectParser.declareString(ConstructingObjectParser.constructorArg(), STATUS_FIELD);

        // ClusterHealthResponse fields
        objectParser.declareString(ConstructingObjectParser.constructorArg(), CLUSTER_NAME_FIELD);
        objectParser.declareInt(ConstructingObjectParser.constructorArg(), NUMBER_OF_PENDING_TASKS_FIELD);
        objectParser.declareInt(ConstructingObjectParser.constructorArg(), NUMBER_OF_IN_FLIGHT_FETCH_FIELD);
        objectParser.declareInt(ConstructingObjectParser.constructorArg(), DELAYED_UNASSIGNED_SHARDS_FIELD);
        objectParser.declareLong(ConstructingObjectParser.constructorArg(), TASK_MAX_WAIT_TIME_IN_QUEUE_IN_MILLIS_FIELD);
        objectParser.declareBoolean(ConstructingObjectParser.constructorArg(), TIMED_OUT_FIELD);

        return objectParser.apply(parser, null);
    }

    protected void processClusterUpdateSettingsAction(final ClusterUpdateSettingsAction action, final ClusterUpdateSettingsRequest request,
            final ActionListener<ClusterUpdateSettingsResponse> listener) {
        String source = null;
        try {
            final XContentBuilder builder = request.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS);
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to parse a request.", e);
        }

        getCurlRequest(PUT, "/_cluster/settings").body(source).execute(response -> {
            if (response.getHttpStatusCode() != 200) {
                throw new ElasticsearchException("Indices are not found: " + response.getHttpStatusCode());
            }
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final ClusterUpdateSettingsResponse clusterUpdateSettingsResponse = ClusterUpdateSettingsResponse.fromXContent(parser);
                listener.onResponse(clusterUpdateSettingsResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }

    protected void processMainAction(final MainAction action, final MainRequest request, final ActionListener<MainResponse> listener) {
        getCurlRequest(POST, "/_xpack").execute(response -> {
            if (response.getHttpStatusCode() != 200) {
                throw new ElasticsearchException("Indices are not found: " + response.getHttpStatusCode());
            }
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final MainResponse mainResponse = MainResponse.fromXContent(parser);
                listener.onResponse(mainResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }

    protected void processUpdateSettingsAction(final UpdateSettingsAction action, final UpdateSettingsRequest request,
            final ActionListener<UpdateSettingsResponse> listener) {
        String source = null;
        try {
            final XContentBuilder builder = request.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS);
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to parse a request.", e);
        }

        getCurlRequest(PUT, "/_settings", request.indices()).param("preserve_existing", String.valueOf(request.isPreserveExisting()))
                .body(source).execute(response -> {
                    if (response.getHttpStatusCode() != 200) {
                        throw new ElasticsearchException("Indices are not found: " + response.getHttpStatusCode());
                    }
                    try (final InputStream in = response.getContentAsStream()) {
                        final XContentParser parser = createParser(in);
                        final UpdateSettingsResponse updateSettingsResponse = UpdateSettingsResponse.fromXContent(parser);
                        listener.onResponse(updateSettingsResponse);
                    } catch (final Exception e) {
                        listener.onFailure(e);
                    }
                }, listener::onFailure);
    }

    protected void processGetSettingsAction(final GetSettingsAction action, final GetSettingsRequest request,
            final ActionListener<GetSettingsResponse> listener) {
        getCurlRequest(GET, "/_settings", request.indices()).execute(response -> {
            if (response.getHttpStatusCode() != 200) {
                throw new ElasticsearchException("Indices are not found: " + response.getHttpStatusCode());
            }
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final GetSettingsResponse getSettingsResponse = getGetSettingsResponsefromXContent(parser);
                listener.onResponse(getSettingsResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }

    protected GetSettingsResponse getGetSettingsResponsefromXContent(final XContentParser parser) throws IOException {
        final HashMap<String, Settings> indexToSettings = new HashMap<>();

        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        parser.nextToken();

        while (!parser.isClosed()) {
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                parseIndexEntry(parser, indexToSettings);
            } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                parser.skipChildren();
            } else {
                parser.nextToken();
            }
        }

        final ImmutableOpenMap<String, Settings> settingsMap =
                ImmutableOpenMap.<String, Settings> builder().putAll(indexToSettings).build();

        return new GetSettingsResponse(settingsMap);
    }

    protected void parseIndexEntry(final XContentParser parser, final Map<String, Settings> indexToSettings) throws IOException {
        final String indexName = parser.currentName();
        parser.nextToken();
        while (!parser.isClosed() && parser.currentToken() != XContentParser.Token.END_OBJECT) {
            parseSettingsField(parser, indexName, indexToSettings);
        }
    }

    protected void parseSettingsField(final XContentParser parser, final String currentIndexName,
            final Map<String, Settings> indexToSettings) throws IOException {
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            if (SETTINGS_FIELD.match(parser.currentName(), LoggingDeprecationHandler.INSTANCE)) {
                indexToSettings.put(currentIndexName, Settings.fromXContent(parser));
            } else {
                parser.skipChildren();
            }
        } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
            parser.skipChildren();
        }
        parser.nextToken();
    }

    protected void processForceMergeAction(final ForceMergeAction action, final ForceMergeRequest request,
            final ActionListener<ForceMergeResponse> listener) {
        getCurlRequest(POST, "/_forcemerge", request.indices()).param("max_num_segments", String.valueOf(request.maxNumSegments()))
                .param("only_expunge_deletes", String.valueOf(request.onlyExpungeDeletes()))
                .param("flush", String.valueOf(request.flush())).execute(response -> {
                    if (response.getHttpStatusCode() != 200) {
                        throw new ElasticsearchException("Indices are not found: " + response.getHttpStatusCode());
                    }
                    try (final InputStream in = response.getContentAsStream()) {
                        final XContentParser parser = createParser(in);
                        final ForceMergeResponse forceMergeResponse = ForceMergeResponse.fromXContent(parser);
                        listener.onResponse(forceMergeResponse);
                    } catch (final Exception e) {
                        listener.onFailure(e);
                    }
                }, listener::onFailure);
    }

    protected void processClearScrollAction(final ClearScrollAction action, final ClearScrollRequest request,
            final ActionListener<ClearScrollResponse> listener) {
        String source = null;
        try {
            final XContentBuilder builder =
                    XContentFactory.jsonBuilder().startObject().array("scroll_id", request.getScrollIds().toArray(new String[0]))
                            .endObject();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
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
            source = new String(MultiSearchRequest.writeMultiLineFormat(request, XContentFactory.xContent(XContentType.JSON)));
        } catch (final Exception e) {
            throw new ElasticsearchException("Failed to parse a request.", e);
        }

        getCurlRequest(GET, ContentType.X_NDJSON, "/_msearch").body(source).execute(response -> {
            if (response.getHttpStatusCode() != 200) {
                throw new ElasticsearchException("Content is not found: " + response.getHttpStatusCode());
            }

            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final MultiSearchResponse multiSearchResponse = MultiSearchResponse.fromXContext(parser);
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
            final XContentBuilder builder = request.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS);
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
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

    void processFieldCapabilitiesAction(final FieldCapabilitiesAction action, final FieldCapabilitiesRequest request,
            final ActionListener<FieldCapabilitiesResponse> listener) {
        getCurlRequest(GET, "/_field_caps?fields=" + String.join(",", request.fields()), request.indices()).execute(response -> {
            if (response.getHttpStatusCode() != 200) {
                throw new ElasticsearchException("not found: " + response.getHttpStatusCode());
            }
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final FieldCapabilitiesResponse fieldCapabilitiesResponse = getFieldCapabilitiesResponsefromXContent(parser);
                listener.onResponse(fieldCapabilitiesResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }

    protected FieldCapabilitiesResponse getFieldCapabilitiesResponsefromXContent(final XContentParser parser) {
        // workaround fix
        @SuppressWarnings("unchecked")
        final ConstructingObjectParser<FieldCapabilitiesResponse, Void> objectParser =
                new ConstructingObjectParser<>("field_capabilities_response", true,
                        a -> newFieldCapabilitiesResponse(((List<Tuple<String, Map<String, FieldCapabilities>>>) a[0]).stream().collect(
                                Collectors.toMap(Tuple::v1, Tuple::v2))));

        objectParser.declareNamedObjects(ConstructingObjectParser.constructorArg(), (p, c, n) -> {
            final Map<String, FieldCapabilities> typeToCapabilities = parseTypeToCapabilities(p, n);
            return new Tuple<>(n, typeToCapabilities);
        }, FIELDS_FIELD);

        try {
            return objectParser.parse(parser, null);
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to parse FieldCapabilitiesResponse.", e);
        }
    }

    protected FieldCapabilitiesResponse newFieldCapabilitiesResponse(final Map<String, Map<String, FieldCapabilities>> map) {
        final Class<FieldCapabilitiesResponse> clazz = FieldCapabilitiesResponse.class;
        final Class<?>[] types = { Map.class };
        try {
            final Constructor<FieldCapabilitiesResponse> constructor = clazz.getDeclaredConstructor(types);
            constructor.setAccessible(true);
            return constructor.newInstance(map);
        } catch (final Exception e) {
            throw new ElasticsearchException("Failed to create FieldCapabilitiesResponse.", e);
        }
    }

    protected Map<String, FieldCapabilities> parseTypeToCapabilities(final XContentParser parser, final String name) throws IOException {
        final Map<String, FieldCapabilities> typeToCapabilities = new HashMap<>();

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);
            final String type = parser.currentName();
            final FieldCapabilities capabilities = getFieldCapabilitiesfromXContent(name, parser);
            typeToCapabilities.put(type, capabilities);
        }
        return typeToCapabilities;
    }

    protected FieldCapabilities getFieldCapabilitiesfromXContent(final String sname, final XContentParser parser) throws IOException {
        @SuppressWarnings("unchecked")
        final ConstructingObjectParser<FieldCapabilities, String> objectParser =
                new ConstructingObjectParser<>("field_capabilities", true, (a, name) -> newFieldCapabilities(name, (String) a[0], true,
                        true, (a[3] != null ? ((List<String>) a[3]).toArray(new String[0]) : null),
                        (a[4] != null ? ((List<String>) a[4]).toArray(new String[0]) : null),
                        (a[5] != null ? ((List<String>) a[5]).toArray(new String[0]) : null)));

        objectParser.declareString(ConstructingObjectParser.constructorArg(), TYPE_FIELD);
        objectParser.declareBoolean(ConstructingObjectParser.constructorArg(), SEARCHABLE_FIELD);
        objectParser.declareBoolean(ConstructingObjectParser.constructorArg(), AGGREGATABLE_FIELD);
        objectParser.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), INDICES_FIELD);
        objectParser.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), NON_SEARCHABLE_INDICES_FIELD);
        objectParser.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), NON_AGGREGATABLE_INDICES_FIELD);

        return objectParser.parse(parser, sname);
    }

    protected FieldCapabilities newFieldCapabilities(final String name, final String type, final boolean isSearchable,
            final boolean isAggregatable, final String[] indices, final String[] nonSearchableIndices, final String[] nonAggregatableIndices) {
        final Class<FieldCapabilities> clazz = FieldCapabilities.class;
        final Class<?>[] types =
                { String.class, String.class, boolean.class, boolean.class, String[].class, String[].class, String[].class };
        try {
            final Constructor<FieldCapabilities> constructor = clazz.getDeclaredConstructor(types);
            constructor.setAccessible(true);
            return constructor.newInstance(name, type, isSearchable, isAggregatable, indices, nonSearchableIndices, nonAggregatableIndices);
        } catch (final Exception e) {
            throw new ElasticsearchException("Failed to create ConstructingObjectParser.", e);
        }
    }

    protected void processBulkAction(final BulkAction action, final BulkRequest request, final ActionListener<BulkResponse> listener) {
        // http://ndjson.org/
        final StringBuilder buf = new StringBuilder(10000);
        try {
            @SuppressWarnings("rawtypes")
            final List<DocWriteRequest> bulkRequests = request.requests();
            for (@SuppressWarnings("rawtypes")
            final DocWriteRequest req : bulkRequests) {
                buf.append(getStringfromDocWriteRequest(req));
                buf.append('\n');
                switch (req.opType().getId()) {
                case 0: { // INDEX
                    buf.append(XContentHelper.convertToJson(((IndexRequest) req).source(), false, XContentType.JSON));
                    buf.append('\n');
                    break;
                }
                case 1: { // CREATE
                    final XContentBuilder builder =
                            ((CreateIndexRequest) req).toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS);
                    buf.append(BytesReference.bytes(builder).utf8ToString());
                    buf.append('\n');
                    break;
                }
                case 2: { // UPDATE
                    final XContentBuilder builder =
                            ((UpdateRequest) req).toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS);
                    buf.append(BytesReference.bytes(builder).utf8ToString());
                    buf.append('\n');
                    break;
                }
                case 3: { // DELETE
                    break;
                }
                default:
                    break;
                }
            }
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to parse a request.", e);
        }

        getCurlRequest(POST, "/_bulk").body(buf.toString()).execute(response -> {
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

    protected String getStringfromDocWriteRequest(final DocWriteRequest<?> request) {
        StringBuilder sb = new StringBuilder(100).append("{\"");
        sb.append(request.opType().getLowercase()).append("\":{\"").append(_INDEX_FIELD).append("\":\"").append(request.index())
                .append("\",\"").append(_TYPE_FIELD).append("\":\"").append(request.type()).append("\",\"").append(_ID_FIELD)
                .append("\":\"").append(request.id()).append("\",\"").append(_ROUTING_FIELD).append("\":\"").append(request.routing())
                .append("\",\"").append(_VERSION_FIELD).append("\":\"").append(request.version()).append("\"}}");
        return sb.toString();
    }

    protected void processGetAction(final GetAction action, final GetRequest request, final ActionListener<GetResponse> listener) {
        getCurlRequest(GET, "/" + request.type() + "/" + request.id(), request.index()).param("routing", request.routing())
                .param("preference", request.preference()).execute(response -> {
                    try (final InputStream in = response.getContentAsStream()) {
                        if (response.getHttpStatusCode() != 200) {
                            throw new ElasticsearchException("Content is not found: " + response.getHttpStatusCode());
                        }
                        final XContentParser parser = createParser(in);
                        final GetResponse getResponse = GetResponse.fromXContent(parser);
                        listener.onResponse(getResponse);
                    } catch (final Exception e) {
                        listener.onFailure(e);
                    }
                }, listener::onFailure);
    }

    protected void processMultiGetAction(final MultiGetAction action, final MultiGetRequest request,
            final ActionListener<MultiGetResponse> listener) {
        String source = null;
        try {
            final XContentBuilder builder = request.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS);
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to parse a request.", e);
        }
        getCurlRequest(GET, "/_mget").body(source).execute(response -> {
            try (final InputStream in = response.getContentAsStream()) {
                if (response.getHttpStatusCode() != 200) {
                    throw new ElasticsearchException("not found: " + response.getHttpStatusCode());
                }
                final XContentParser parser = createParser(in);
                final MultiGetResponse multiGetResponse = MultiGetResponse.fromXContent(parser);
                listener.onResponse(multiGetResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }

    protected void processIndexAction(final IndexAction action, final IndexRequest request, final ActionListener<IndexResponse> listener) {
        String source = null;

        try {
            source = XContentHelper.convertToJson(request.source(), false, XContentType.JSON);
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to parse a request.", e);
        }

        getCurlRequest(PUT, "/" + request.type() + "/" + request.id(), request.index()).param("routing", request.routing())
        //.param("op_type", "create")
                .body(source).execute(response -> {
                    try (final InputStream in = response.getContentAsStream()) {
                        if (response.getHttpStatusCode() != 200 && response.getHttpStatusCode() != 201) {
                            throw new ElasticsearchException("not found: " + response.getHttpStatusCode());
                        }
                        final XContentParser parser = createParser(in);
                        final IndexResponse indexResponse = IndexResponse.fromXContent(parser);
                        listener.onResponse(indexResponse);
                    } catch (final Exception e) {
                        listener.onFailure(e);
                    }
                }, listener::onFailure);
    }

    protected void processUpdateAction(final UpdateAction action, final UpdateRequest request, final ActionListener<UpdateResponse> listener) {
        String source = null;
        try {
            final XContentBuilder builder = request.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS);
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to parse a request.", e);
        }

        getCurlRequest(POST, "/" + request.type() + "/" + request.id() + "/_update", request.index()).param("routing", request.routing())
                .param("retry_on_conflict", String.valueOf(request.retryOnConflict())).param("version", String.valueOf(request.version()))
                .body(source).execute(response -> {
                    try (final InputStream in = response.getContentAsStream()) {
                        if (response.getHttpStatusCode() != 200 && response.getHttpStatusCode() != 201) {
                            throw new ElasticsearchException("not found: " + response.getHttpStatusCode());
                        }
                        final XContentParser parser = createParser(in);
                        final UpdateResponse updateResponse = UpdateResponse.fromXContent(parser);
                        listener.onResponse(updateResponse);
                    } catch (final Exception e) {
                        listener.onFailure(e);
                    }
                }, listener::onFailure);
    }

    protected void processExplainAction(final ExplainAction action, final ExplainRequest request,
            final ActionListener<ExplainResponse> listener) {
        String source = null;
        try {
            final XContentBuilder builder =
                    XContentFactory.jsonBuilder().startObject().field(QUERY_FIELD.getPreferredName(), request.query()).endObject();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to parse a request.", e);
        }
        getCurlRequest(POST, "/" + request.type() + "/" + request.id() + "/_explain", request.index()).param("routing", request.routing())
                .param("preference", request.preference()).body(source).execute(response -> {
                    try (final InputStream in = response.getContentAsStream()) {
                        if (response.getHttpStatusCode() != 200) {
                            throw new ElasticsearchException("not found: " + response.getHttpStatusCode());
                        }
                        final XContentParser parser = createParser(in);
                        final ExplainResponse explainResponse = getExplainResponsefromXContent(parser);
                        listener.onResponse(explainResponse);
                    } catch (final Exception e) {
                        listener.onFailure(e);
                    }
                }, listener::onFailure);
    }

    protected ExplainResponse getExplainResponsefromXContent(final XContentParser parser) {
        final ConstructingObjectParser<ExplainResponse, Boolean> objectParser =
                new ConstructingObjectParser<>("explain", true, (arg, exists) -> new ExplainResponse((String) arg[0], (String) arg[1],
                        (String) arg[2], exists, (Explanation) arg[3], (GetResult) arg[4]));

        objectParser.declareString(ConstructingObjectParser.constructorArg(), _INDEX_FIELD);
        objectParser.declareString(ConstructingObjectParser.constructorArg(), _TYPE_FIELD);
        objectParser.declareString(ConstructingObjectParser.constructorArg(), _ID_FIELD);
        objectParser.declareObject(ConstructingObjectParser.optionalConstructorArg(), getExplanationParser(), EXPLANATION_FIELD);
        objectParser.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> GetResult.fromXContentEmbedded(p),
                GET_FIELD);

        return objectParser.apply(parser, true);
    }

    protected ConstructingObjectParser getExplanationParser() {
        @SuppressWarnings("unchecked")
        final ConstructingObjectParser<Explanation, Boolean> explanationParser =
                new ConstructingObjectParser<>("explanation", true, arg -> {
                    if ((float) arg[0] > 0) {
                        return Explanation.match((float) arg[0], (String) arg[1], (Collection<Explanation>) arg[2]);
                    } else {
                        return Explanation.noMatch((String) arg[1], (Collection<Explanation>) arg[2]);
                    }
                });
        explanationParser.declareFloat(ConstructingObjectParser.constructorArg(), VALUE_FIELD);
        explanationParser.declareString(ConstructingObjectParser.constructorArg(), DESCRIPTION_FIELD);
        explanationParser.declareObjectArray(ConstructingObjectParser.constructorArg(), explanationParser, DETAILS_FIELD);
        return explanationParser;
    }

    protected void processDeleteAction(final DeleteAction action, final DeleteRequest request, final ActionListener<DeleteResponse> listener) {
        getCurlRequest(DELETE, "/" + request.type() + "/" + request.id(), request.index()).param("routing", request.routing())
                .param("version", String.valueOf(request.version())).execute(response -> {
                    try (final InputStream in = response.getContentAsStream()) {
                        if (response.getHttpStatusCode() != 200) {
                            throw new ElasticsearchException("not found: " + response.getHttpStatusCode());
                        }
                        final XContentParser parser = createParser(in);
                        final DeleteResponse deleteResponse = DeleteResponse.fromXContent(parser);
                        listener.onResponse(deleteResponse);
                    } catch (final Exception e) {
                        listener.onFailure(e);
                    }
                }, listener::onFailure);
    }

    protected void processCreateIndexAction(final CreateIndexAction action, final CreateIndexRequest request,
            final ActionListener<CreateIndexResponse> listener) {
        String source = null;
        try {
            final XContentBuilder builder = request.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS);
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
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
                //                final RefreshResponse refreshResponse = getResponseFromXContent(parser);
                final RefreshResponse refreshResponse = RefreshResponse.fromXContent(parser);
                listener.onResponse(refreshResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }

    protected void processSearchAction(final SearchAction action, final SearchRequest request, final ActionListener<SearchResponse> listener) {
        getCurlRequest(POST,
                (request.types() != null && request.types().length > 0 ? ("/" + String.join(",", request.types())) : "") + "/_search",
                request.indices())
                .param("scroll",
                        (request.scroll() != null && request.scroll().keepAlive() != null) ? request.scroll().keepAlive().toString() : null)
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
        getCurlRequest(HEAD, null, request.indices()).execute(response -> {
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
            final XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startArray("actions");
            for (final AliasActions aliasAction : request.getAliasActions()) {
                builder.startObject().startObject(aliasAction.actionType().toString().toLowerCase());
                builder.array("indices", aliasAction.indices());
                builder.array("aliases", aliasAction.aliases());
                if (aliasAction.filter() != null) {
                    builder.field("filter", aliasAction.filter());
                }
                if (aliasAction.indexRouting() != null) {
                    builder.field("index_routing", aliasAction.indexRouting());
                }
                if (aliasAction.searchRouting() != null) {
                    builder.field("search_routing", aliasAction.searchRouting());
                }
                builder.endObject().endObject();
            }
            builder.endArray().endObject();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
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
                        //                        final FlushResponse flushResponse = getResponseFromXContent(parser, action::newResponse);
                        final FlushResponse flushResponse = FlushResponse.fromXContent(parser);
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
                if (FAILURES_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
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
                if (TOTAL_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    totalShards = parser.intValue();
                } else if (SUCCESSFUL_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    successfulShards = parser.intValue();
                } else if (FAILED_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
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
        final List<String> indices = new ArrayList<>();
        final ImmutableOpenMap.Builder<String, List<AliasMetaData>> aliasesMapBuilder = ImmutableOpenMap.builder();
        final ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> mappingsMapBuilder = ImmutableOpenMap.builder();
        final ImmutableOpenMap.Builder<String, Settings> settingsMapBuilder = ImmutableOpenMap.builder();

        String index = null;
        XContentParser.Token token = parser.nextToken();
        while ((token = parser.nextToken()) != Token.END_OBJECT) {
            if (token == Token.FIELD_NAME) {
                index = parser.currentName();
                indices.add(index);
            } else if (token == Token.START_OBJECT) {
                while (parser.nextToken() == Token.FIELD_NAME) {
                    final String currentFieldName = parser.currentName();
                    if (ALIASES_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                        aliasesMapBuilder.put(index, getAliasesFromXContent(parser));
                    } else if (MAPPINGS_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                        mappingsMapBuilder.put(index, getMappingsFromXContent(parser));
                    } else if (SETTINGS_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                        settingsMapBuilder.put(index, getSettingsFromXContent(parser));
                    }
                }
            }
        }

        final ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = mappingsMapBuilder.build();
        final ImmutableOpenMap<String, List<AliasMetaData>> aliases = aliasesMapBuilder.build();
        final ImmutableOpenMap<String, Settings> settings = settingsMapBuilder.build();

        try (ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
            out.writeStringArray(indices.toArray(new String[indices.size()]));
            out.writeVInt(mappings.size());
            for (final ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> indexEntry : mappings) {
                out.writeString(indexEntry.key);
                out.writeVInt(indexEntry.value.size());
                for (final ObjectObjectCursor<String, MappingMetaData> mappingEntry : indexEntry.value) {
                    out.writeString(mappingEntry.key);
                    mappingEntry.value.writeTo(out);
                }
            }
            out.writeVInt(aliases.size());
            for (final ObjectObjectCursor<String, List<AliasMetaData>> indexEntry : aliases) {
                out.writeString(indexEntry.key);
                out.writeVInt(indexEntry.value.size());
                for (final AliasMetaData aliasEntry : indexEntry.value) {
                    aliasEntry.writeTo(out);
                }
            }
            out.writeVInt(settings.size());
            for (final ObjectObjectCursor<String, Settings> indexEntry : settings) {
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
        final ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> indexMapBuilder = ImmutableOpenMap.builder();

        String index = null;
        Token token = parser.nextToken();
        if (token != null) {
            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                if (token == Token.FIELD_NAME) {
                    index = parser.currentName();
                } else if (token == Token.START_OBJECT) {
                    while (parser.nextToken() == Token.FIELD_NAME) {
                        if (MAPPINGS_FIELD.match(parser.currentName(), LoggingDeprecationHandler.INSTANCE)) {
                            indexMapBuilder.put(index, getMappingsFromXContent(parser));
                            break;
                        } else {
                            parser.skipChildren();
                        }
                    }
                }
            }
        }
        final ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = indexMapBuilder.build();

        try (ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
            out.writeVInt(mappings.size());
            for (final ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> indexEntry : mappings) {
                out.writeString(indexEntry.key);
                out.writeVInt(indexEntry.value.size());
                for (final ObjectObjectCursor<String, MappingMetaData> typeEntry : indexEntry.value) {
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
        final List<AliasMetaData> aliases = new ArrayList<>();
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
        final ImmutableOpenMap.Builder<String, MappingMetaData> mappingsBuilder = ImmutableOpenMap.builder();
        String type = null;
        Token token = parser.nextToken();
        if (token == null) {
            return mappingsBuilder.build();
        }
        while ((token = parser.nextToken()) != Token.END_OBJECT) {
            if (token == Token.FIELD_NAME) {
                type = parser.currentName();
            } else if (token == Token.START_OBJECT) {
                final Map<String, Object> mapping = parser.mapOrdered();
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
                    if (ACKNOWLEDGED_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
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
                if (SHARD_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    shardId = parser.intValue();
                } else if (INDEX_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
                    index = parser.text();
                } else if (REASON_FIELD.match(currentFieldName, LoggingDeprecationHandler.INSTANCE)) {
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
        return xContent.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, in);
    }

    protected String getHost() {
        return hosts[0];
    }

    protected CurlRequest getCurlRequest(final Function<String, CurlRequest> method, final String path, final String... indices) {
        return getCurlRequest(method, ContentType.JSON, path, indices);
    }

    protected CurlRequest getCurlRequest(final Function<String, CurlRequest> method, final ContentType contentType, final String path,
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
