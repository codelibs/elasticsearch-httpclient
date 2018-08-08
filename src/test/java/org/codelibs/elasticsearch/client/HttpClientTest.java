package org.codelibs.elasticsearch.client;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushResponse;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.main.MainAction;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class HttpClientTest {
    static final Logger logger = Logger.getLogger(HttpClientTest.class.getName());

    static ElasticsearchClusterRunner runner;

    static String clusterName;

    private HttpClient client;

    @BeforeAll
    static void setUpAll() {
        clusterName = "es-cl-run-" + System.currentTimeMillis();
        // create runner instance
        runner = new ElasticsearchClusterRunner();
        // create ES nodes
        runner.onBuild((number, settingsBuilder) -> {
            settingsBuilder.put("http.cors.enabled", true);
            settingsBuilder.put("http.cors.allow-origin", "*");
            settingsBuilder.putList("discovery.zen.ping.unicast.hosts", "localhost:9301-9305");
        }).build(newConfigs().clusterName(clusterName).numOfNode(1));

        // wait for yellow status
        runner.ensureYellow();
    }

    @BeforeEach
    void setUp() {
        final Settings settings = Settings.builder().putList("http.hosts", "localhost:9201").build();
        client = new HttpClient(settings, null);
    }

    @AfterEach
    void tearDown() {
        client.close();
    }

    @AfterAll
    static void tearDownAll() {
        // close runner
        try {
            runner.close();
        } catch (final IOException e) {
            // ignore
        }
        // delete all files
        runner.clean();
    }

    @Test
    void test_refresh() throws Exception {
        final String index = "test_refresh";
        CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate(index).execute().actionGet();

        client.admin().indices().prepareRefresh(index).execute(wrap(res -> {
            assertEquals(res.getStatus(), RestStatus.OK);
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            RefreshResponse refreshResponse = client.admin().indices().prepareRefresh(index).execute().actionGet();
            assertEquals(refreshResponse.getStatus(), RestStatus.OK);
        }
    }

    @Test
    void test_search() throws Exception {
        final String index = "test_search";
        CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate(index).execute().actionGet();

        client.prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).execute(wrap(res -> {
            assertEquals(0, res.getHits().getTotalHits());
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            SearchResponse searchResponse = client.prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
            assertEquals(0, searchResponse.getHits().getTotalHits());
        }
    }

    @Test
    void test_create_index() throws Exception {
        final String index1 = "test_create_index1";
        final String index2 = "test_create_index2";
        CountDownLatch latch = new CountDownLatch(1);

        client.admin().indices().prepareCreate(index1).execute(wrap(res -> {
            assertTrue(res.isAcknowledged());
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            CreateIndexResponse createIndexResponse = client.admin().indices().prepareCreate(index2).execute().actionGet();
            assertTrue(createIndexResponse.isAcknowledged());
        }
    }

    @Test
    void test_delete_index() throws Exception {
        final String index1 = "test_delete_index1";
        final String index2 = "test_delete_index2";
        CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate(index1).execute().actionGet();

        client.admin().indices().prepareDelete(index1).execute(wrap(res -> {
            assertTrue(res.isAcknowledged());
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            client.admin().indices().prepareCreate(index2).execute().actionGet();
            DeleteIndexResponse deleteIndexResponse = client.admin().indices().prepareDelete(index2).execute().actionGet();
            assertTrue(deleteIndexResponse.isAcknowledged());
        }
    }

    @Test
    void test_get_index() throws Exception {
        final String index = "test_get_index";
        final String type = "test_type";
        final String alias = "test_alias";
        final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()//
                .startObject()//
                .startObject("properties")//
                .startObject("test_prop")//
                .field("type", "text")//
                .endObject()//
                .endObject()//
                .endObject();
        final String source = BytesReference.bytes(mappingBuilder).utf8ToString();
        CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate(index).execute().actionGet();
        client.admin().indices().prepareAliases().addAlias(index, alias).execute().actionGet();
        client.admin().indices().preparePutMapping(index).setType(type).setSource(source, XContentType.JSON).execute().actionGet();

        client.admin().indices().prepareGetIndex().addIndices(index).execute(wrap(res -> {
            assertEquals(index, res.getIndices()[0]);
            assertTrue(res.getAliases().containsKey(index));
            assertTrue(res.getMappings().containsKey(index));
            assertTrue(res.getSettings().containsKey(index));
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            GetIndexResponse getIndexResponse = client.admin().indices().prepareGetIndex().addIndices(index).execute().actionGet();
            assertEquals(index, getIndexResponse.getIndices()[0]);
            assertTrue(getIndexResponse.getAliases().containsKey(index));
            assertTrue(getIndexResponse.getMappings().containsKey(index));
            assertTrue(getIndexResponse.getSettings().containsKey(index));
        }
    }

    @Test
    void test_open_index() throws Exception {
        final String index1 = "test_open_index1";
        final String index2 = "test_open_index2";
        CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate(index1).execute().actionGet();

        client.admin().indices().prepareOpen(index1).execute(wrap(res -> {
            assertTrue(res.isAcknowledged());
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            client.admin().indices().prepareCreate(index2).execute().actionGet();
            OpenIndexResponse openIndexResponse = client.admin().indices().prepareOpen(index2).execute().actionGet();
            assertTrue(openIndexResponse.isAcknowledged());
        }
    }

    @Test
    void test_close_index() throws Exception {
        final String index1 = "test_close_index1";
        final String index2 = "test_close_index2";
        CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate(index1).execute().actionGet();

        client.admin().indices().prepareClose(index1).execute(wrap(res -> {
            assertTrue(res.isAcknowledged());
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            client.admin().indices().prepareCreate(index2).execute().actionGet();
            CloseIndexResponse closeIndexResponse = client.admin().indices().prepareClose(index2).execute().actionGet();
            assertTrue(closeIndexResponse.isAcknowledged());
        }
    }

    @Test
    void test_indices_exists() throws Exception {
        final String index1 = "test_indices_exists1";
        final String index2 = "test_indices_exists2";
        CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate(index1).execute().actionGet();

        client.admin().indices().prepareExists(index1).execute(wrap(res -> {
            assertTrue(res.isExists());
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            IndicesExistsResponse indicesExistsResponse = client.admin().indices().prepareExists(index1).execute().actionGet();
            assertTrue(indicesExistsResponse.isExists());
            indicesExistsResponse = client.admin().indices().prepareExists(index2).execute().actionGet();
            assertFalse(indicesExistsResponse.isExists());
        }
    }

    @Test
    void test_indices_aliases() throws Exception {
        final String index = "test_indices_aliases";
        final String alias1 = "test_alias1";
        final String alias2 = "test_alias2";
        CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate(index).execute().actionGet();

        client.admin().indices().prepareAliases().addAlias(index, alias1).execute(wrap(res -> {
            assertTrue(res.isAcknowledged());
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            IndicesAliasesResponse indicesAliasesResponse =
                    client.admin().indices().prepareAliases().addAlias(index, alias2).execute().actionGet();
            assertTrue(indicesAliasesResponse.isAcknowledged());
        }
    }

    @Test
    void test_put_mapping() throws Exception {
        final String index1 = "test_put_mapping1";
        final String index2 = "test_put_mapping2";
        final String type = "test_type";
        final XContentBuilder mappingBuilder =
                XContentFactory.jsonBuilder().startObject().startObject("properties").startObject("test_prop").field("type", "text")
                        .endObject().endObject().endObject();
        final String source = BytesReference.bytes(mappingBuilder).utf8ToString();
        CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate(index1).execute().actionGet();

        client.admin().indices().preparePutMapping(index1).setType(type).setSource(source, XContentType.JSON).execute(wrap(res -> {
            assertTrue(res.isAcknowledged());
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            client.admin().indices().prepareCreate(index2).execute().actionGet();
            PutMappingResponse putMappingResponse =
                    client.admin().indices().preparePutMapping(index2).setType(type).setSource(source, XContentType.JSON).execute()
                            .actionGet();
            assertTrue(putMappingResponse.isAcknowledged());
        }
    }

    @Test
    void test_get_mappings() throws Exception {
        final String index = "test_get_mappings1";
        final String type = "test_type";
        final XContentBuilder mappingBuilder =
                XContentFactory.jsonBuilder().startObject().startObject("properties").startObject("test_prop").field("type", "text")
                        .endObject().endObject().endObject();
        String source = BytesReference.bytes(mappingBuilder).utf8ToString();
        Map<String, Object> mappingMap = XContentHelper.convertToMap(BytesReference.bytes(mappingBuilder), true, XContentType.JSON).v2();
        MappingMetaData mappingMetaData = new MappingMetaData(type, mappingMap);
        CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate(index).execute().actionGet();
        client.admin().indices().preparePutMapping(index).setType(type).setSource(source, XContentType.JSON).execute().actionGet();

        client.admin().indices().prepareGetMappings(index).execute(wrap(res -> {
            ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = res.getMappings();
            assertTrue(mappings.containsKey(index));
            assertTrue(mappings.get(index).containsKey(type));
            assertEquals(mappings.get(index).get(type), mappingMetaData);
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            GetMappingsResponse getMappingsResponse = client.admin().indices().prepareGetMappings(index).execute().actionGet();
            ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = getMappingsResponse.getMappings();
            assertTrue(mappings.containsKey(index));
            assertTrue(mappings.get(index).containsKey(type));
            assertEquals(mappings.get(index).get(type), mappingMetaData);
        }
    }

    @Test
    void test_flush() throws Exception {
        final String index = "test_flush";
        CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate(index).execute().actionGet();

        client.admin().indices().prepareFlush(index).execute(wrap(res -> {
            assertEquals(res.getStatus(), RestStatus.OK);
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            FlushResponse res = client.admin().indices().prepareFlush(index).execute().actionGet();
            assertEquals(res.getStatus(), RestStatus.OK);
        }
    }

    @Test
    void test_scroll() throws Exception {
        final long NUM = 2;
        final String index = "test_scroll";
        final String type = "test_type";
        final BulkRequestBuilder bulkRequestBuilder1 = client.prepareBulk();
        for (int i = 1; i <= NUM; i++) {
            bulkRequestBuilder1.add(client.prepareIndex(index, type, String.valueOf(i)).setSource(
                    "{ \"test\" :" + "\"test" + String.valueOf(i) + "\" }", XContentType.JSON));
        }
        bulkRequestBuilder1.execute().actionGet();
        client.admin().indices().prepareRefresh(index).execute().actionGet();
        SearchResponse scrollResponse =
                client.prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).setScroll(new TimeValue(60000)).setSize(1).execute()
                        .actionGet();
        String id = scrollResponse.getScrollId();
        SearchHit[] hits = scrollResponse.getHits().getHits();
        while (hits.length != 0) {
            assertEquals(1, hits.length);
            scrollResponse = client.prepareSearchScroll(id).setScroll(new TimeValue(60000)).execute().actionGet();
            hits = scrollResponse.getHits().getHits();
        }

        // Test CrealScroll API
        ClearScrollResponse clearScrollResponse = client.prepareClearScroll().addScrollId(id).execute().actionGet();
        assertTrue(clearScrollResponse.isSucceeded());
    }

    @Test
    void test_multi_search() throws Exception {
        final SearchRequestBuilder srb1 = client.prepareSearch().setQuery(QueryBuilders.queryStringQuery("word")).setSize(1);
        final SearchRequestBuilder srb2 = client.prepareSearch().setQuery(QueryBuilders.matchQuery("name", "test")).setSize(1);
        CountDownLatch latch = new CountDownLatch(1);

        try {
            client.prepareMultiSearch().add(srb1).add(srb2).execute(wrap(res -> {
                long nbHits = 0;
                for (MultiSearchResponse.Item item : res.getResponses()) {
                    SearchResponse searchResponse = item.getResponse();
                    nbHits += searchResponse.getHits().getTotalHits();
                }
                assertEquals(0, nbHits);
                latch.countDown();
            }, e -> {
                latch.countDown();
            }));
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }

        {
            MultiSearchResponse res = client.prepareMultiSearch().add(srb1).add(srb2).execute().actionGet();
            long nbHits = 0;
            for (MultiSearchResponse.Item item : res.getResponses()) {
                SearchResponse searchResponse = item.getResponse();
                nbHits += searchResponse.getHits().getTotalHits();
            }
            assertEquals(0, nbHits);
        }
    }

    @Test
    void test_crud0() throws Exception {
        final String index = "test_crud_index";
        final String type = "test_type";
        final String id = "1";

        // Create a document
        final IndexResponse indexResponse =
                client.prepareIndex(index, type, id)
                        .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                        .setSource("{" + "\"user\":\"user_" + id + "\"," + "\"postDate\":\"2018-07-30\"," + "\"text\":\"test\"" + "}",
                                XContentType.JSON).execute().actionGet();
        assertTrue((Result.CREATED == indexResponse.getResult()) || (Result.UPDATED == indexResponse.getResult()));

        // Refresh index to search
        client.admin().indices().prepareRefresh(index).execute().actionGet();

        // Search the document
        final SearchResponse searchResponse =
                client.prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).setSize(0).execute().actionGet();
        assertEquals(1, searchResponse.getHits().getTotalHits());

        // Get the document
        final GetResponse getResponse = client.prepareGet(index, type, id).execute().actionGet();
        assertTrue(getResponse.isExists());

        // Update the document
        final UpdateResponse updateResponse = client.prepareUpdate(index, type, id).setDoc("foo", "bar").execute().actionGet();
        assertEquals(Result.UPDATED, updateResponse.getResult());

        // Delete the document
        final DeleteResponse deleteResponse = client.prepareDelete(index, type, id).execute().actionGet();
        assertEquals(RestStatus.OK, deleteResponse.status());

        // check the document deleted
        assertThrows(ElasticsearchException.class, () -> client.prepareGet(index, type, id).execute().actionGet());
        // final GetResponse getResponse2 = client.prepareGet(index, type, id).execute().actionGet(); // CurlException
        // assertFalse(getResponse.isExists());
    }

    @Test
    void test_crud1() throws Exception {
        final long NUM = 10;
        final String index = "test_bulk_multi";
        final String type = "test_type";

        // Create documents with Bulk API
        final BulkRequestBuilder bulkRequestBuilder1 = client.prepareBulk();
        for (int i = 1; i <= NUM; i++) {
            bulkRequestBuilder1.add(client.prepareIndex(index, type, String.valueOf(i)).setSource(
                    "{ \"test\" :" + "\"test" + String.valueOf(i) + "\" }", XContentType.JSON));
        }
        final BulkResponse bulkResponse1 = bulkRequestBuilder1.execute().actionGet();
        assertFalse(bulkResponse1.hasFailures());
        client.admin().indices().prepareRefresh(index).execute().actionGet();

        // Search the documents
        final SearchResponse searchResponse1 = client.prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertEquals(NUM, searchResponse1.getHits().getTotalHits());

        // Get the documents with MultiGet API
        MultiGetRequestBuilder mgetRequestBuilder = client.prepareMultiGet();
        for (int i = 1; i <= NUM; i++) {
            mgetRequestBuilder.add(new MultiGetRequest.Item(index, type, String.valueOf(i)));
        }
        final MultiGetResponse mgetResponse = mgetRequestBuilder.execute().actionGet();
        assertEquals(NUM, mgetResponse.getResponses().length);

        // Delete a document
        final DeleteResponse deleteResponse = client.prepareDelete(index, type, "1").execute().actionGet();
        assertEquals(RestStatus.OK, deleteResponse.status());

        client.admin().indices().prepareRefresh(index).execute().actionGet();

        final SearchResponse searchResponse2 = client.prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertEquals(NUM - 1, searchResponse2.getHits().getTotalHits());

        // Delete all the documents with Bulk API
        final BulkRequestBuilder bulkRequestBuilder2 = client.prepareBulk();
        for (int i = 2; i <= NUM; i++) {
            bulkRequestBuilder2.add(client.prepareDelete(index, type, String.valueOf(i)));
        }
        final BulkResponse bulkResponse2 = bulkRequestBuilder2.execute().actionGet();
        assertFalse(bulkResponse2.hasFailures());
    }

    @Test
    void test_explain() throws Exception {
        final String index = "test_explain";
        final String type = "test_type";
        final String id = "1";
        CountDownLatch latch = new CountDownLatch(1);
        client.prepareIndex(index, type, id)
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .setSource("{" + "\"user\":\"user_" + id + "\"," + "\"postDate\":\"2018-07-30\"," + "\"text\":\"test\"" + "}",
                        XContentType.JSON).execute().actionGet();
        client.admin().indices().prepareRefresh(index).execute().actionGet();

        client.prepareExplain(index, type, id).setQuery(QueryBuilders.termQuery("text", "test")).execute(wrap(res -> {
            assertTrue(res.hasExplanation());
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            ExplainResponse explainRespose =
                    client.prepareExplain(index, type, id).setQuery(QueryBuilders.termQuery("text", "test")).execute().actionGet();
            assertTrue(explainRespose.hasExplanation());
        }
    }

    @Test
    void test_field_caps() throws Exception {
        final String index0 = "test_field_caps0";
        final String index1 = "test_field_caps1";
        final String type = "test_type";
        final String id = "1";
        final String field0 = "user";
        final String field1 = "content";
        CountDownLatch latch = new CountDownLatch(1);
        client.prepareIndex(index0, type, id)
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .setSource("{" + "\"user\":\"user_" + id + "\"," + "\"postDate\":\"2018-07-30\"," + "\"" + field1 + "\":1" + "}",
                        XContentType.JSON).execute().actionGet();
        client.prepareIndex(index1, type, id)
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .setSource("{" + "\"user\":\"user_" + id + "\"," + "\"postDate\":\"2018-07-30\"," + "\"" + field1 + "\":\"test\"" + "}",
                        XContentType.JSON).execute().actionGet();

        client.admin().indices().prepareRefresh(index0).execute().actionGet();
        client.admin().indices().prepareRefresh(index1).execute().actionGet();

        client.prepareFieldCaps().setFields(field0).execute(wrap(res -> {
            assertTrue(res.getField(field0) != null);
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            final FieldCapabilitiesResponse fieldCapabilitiesResponse =
                    client.prepareFieldCaps().setFields(field0, field1).execute().actionGet();
            final FieldCapabilities capabilities0 = fieldCapabilitiesResponse.getField(field0).get("text");
            assertEquals(capabilities0.getName(), field0);
            final FieldCapabilities capabilities1 = fieldCapabilitiesResponse.getField(field1).get("long");
            assertEquals(capabilities1.indices().length, 1);
            final FieldCapabilities capabilities2 = fieldCapabilitiesResponse.getField(field1).get("text");
            assertEquals(capabilities2.indices().length, 1);
        }
    }

    @Test
    void test_update_settings() throws Exception {
        final String index = "test_update_settings";
        final String type = "test_type";
        final String id = "1";
        CountDownLatch latch = new CountDownLatch(1);
        client.prepareIndex(index, type, id)
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .setSource("{" + "\"user\":\"user_" + id + "\"," + "\"postDate\":\"2018-07-30\"," + "\"text\":\"test\"" + "}",
                        XContentType.JSON).execute().actionGet();
        client.admin().indices().prepareRefresh(index).execute().actionGet();

        client.admin().indices().prepareUpdateSettings(index).setSettings(Settings.builder().put("index.number_of_replicas", 0))
                .execute(wrap(res -> {
                    assertTrue(res.isAcknowledged());
                    latch.countDown();
                }, e -> {
                    e.printStackTrace();
                    assertTrue(false);
                    latch.countDown();
                }));
        latch.await();

        {
            UpdateSettingsResponse updateSettingsResponse =
                    client.admin().indices().prepareUpdateSettings(index)
                            .setSettings(Settings.builder().put("index.number_of_replicas", 0)).execute().actionGet();
            assertTrue(updateSettingsResponse.isAcknowledged());
        }
    }

    @Test
    void test_get_settings() throws Exception {
        final String index = "test_get_settings";
        final String type = "test_type";
        final String id = "1";
        CountDownLatch latch = new CountDownLatch(1);
        client.prepareIndex(index, type, id)
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .setSource("{" + "\"user\":\"user_" + id + "\"," + "\"postDate\":\"2018-07-30\"," + "\"text\":\"test\"" + "}",
                        XContentType.JSON).execute().actionGet();
        client.admin().indices().prepareRefresh(index).execute().actionGet();

        client.admin().indices().prepareGetSettings(index).execute(wrap(res -> {
            assertTrue(res.getSetting(index, "index.number_of_shards") != null);
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            GetSettingsResponse getSettingsResponse = client.admin().indices().prepareGetSettings(index).execute().actionGet();
            assertTrue(getSettingsResponse.getSetting(index, "index.number_of_shards") != null);
        }
    }

    @Test
    void test_force_merge() throws Exception {
        final String index = "test_force_merge";
        final String type = "test_type";
        final String id = "1";
        CountDownLatch latch = new CountDownLatch(1);
        client.prepareIndex(index, type, id)
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .setSource("{" + "\"user\":\"user_" + id + "\"," + "\"postDate\":\"2018-07-30\"," + "\"text\":\"test\"" + "}",
                        XContentType.JSON).execute().actionGet();
        client.admin().indices().prepareRefresh(index).execute().actionGet();

        client.admin().indices().prepareForceMerge(index).execute(wrap(res -> {
            assertEquals(res.getStatus(), RestStatus.OK);
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            ForceMergeResponse forceMergeResponse = client.admin().indices().prepareForceMerge(index).execute().actionGet();
            assertEquals(forceMergeResponse.getStatus(), RestStatus.OK);
        }
    }

    @Test
    void test_cluster_update_settings() throws Exception {
        final String transientSettingKey = RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey();
        final int transientSettingValue = 40000000;
        final Settings transientSettings = Settings.builder().put(transientSettingKey, transientSettingValue, ByteSizeUnit.BYTES).build();
        CountDownLatch latch = new CountDownLatch(1);

        client.admin().cluster().prepareUpdateSettings().setTransientSettings(transientSettings).execute(wrap(res -> {
            assertTrue(res.isAcknowledged());
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            ClusterUpdateSettingsResponse clusterUpdateSettingsResponse =
                    client.admin().cluster().prepareUpdateSettings().setTransientSettings(transientSettings).execute().actionGet();
            assertTrue(clusterUpdateSettingsResponse.isAcknowledged());
        }
    }

    @Test
    void test_cluster_health() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        client.admin().cluster().prepareHealth().execute(wrap(res -> {
            assertEquals(res.getClusterName(), clusterName);
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            ClusterHealthResponse custerHealthResponse = client.admin().cluster().prepareHealth().execute().actionGet();
            assertEquals(custerHealthResponse.getClusterName(), clusterName);
        }
    }

    @Test
    void test_aliases_exist() throws Exception {
        final String index = "test_aliases_exist";
        final String alias1 = "test_alias1";
        final String alias2 = "test_alias2";
        CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate(index).execute().actionGet();

        client.admin().indices().prepareAliases().addAlias(index, alias1).execute().actionGet();
        client.admin().indices().prepareAliasesExist(alias1).execute(wrap(res -> {
            assertTrue(res.isExists());
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            client.admin().indices().prepareAliases().addAlias(index, alias2).execute().actionGet();
            AliasesExistResponse aliasesExistResponse = client.admin().indices().prepareAliasesExist(alias2).execute().actionGet();
            assertTrue(aliasesExistResponse.isExists());
        }
    }

    @Test
    void test_validate_query() throws Exception {
        final String index = "test_validate_query";
        final String type = "test_type";
        final String id = "0";
        CountDownLatch latch = new CountDownLatch(1);

        client.prepareIndex(index, type, id)
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .setSource("{" + "\"user\":\"user_" + id + "\"," + "\"postDate\":\"2018-07-30\"," + "\"text\":\"test\"" + "}",
                        XContentType.JSON).execute().actionGet();
        client.admin().indices().prepareRefresh(index).execute().actionGet();

        client.admin().indices().prepareValidateQuery(index).setTypes(type).setExplain(true).setQuery(QueryBuilders.matchAllQuery())
                .execute(wrap(res -> {
                    assertTrue(res.isValid());
                    latch.countDown();
                }, e -> {
                    e.printStackTrace();
                    assertTrue(false);
                    latch.countDown();
                }));
        latch.await();

        {
            ValidateQueryResponse validateQueryResponse =
                    client.admin().indices().prepareValidateQuery(index).setTypes(type).setExplain(true)
                            .setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
            assertTrue(validateQueryResponse.isValid());
        }
    }

    @Test
    void test_pending_cluster_tasks() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        client.admin().cluster().preparePendingClusterTasks().execute(wrap(res -> {
            assertTrue(res.getPendingTasks() != null);
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            PendingClusterTasksResponse pendingClusterTasksResponse =
                    client.admin().cluster().preparePendingClusterTasks().execute().actionGet();
            assertTrue(pendingClusterTasksResponse.getPendingTasks() != null);
        }
    }

    @Test
    void test_get_aliases() throws Exception {
        final String index = "test_get_aliases";
        final String alias1 = "test_alias1";
        final String alias2 = "test_alias2";
        CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate(index).execute().actionGet();
        client.admin().indices().prepareAliases().addAlias(index, alias1).execute().actionGet();
        client.admin().indices().prepareRefresh(index).execute().actionGet();

        client.admin().indices().prepareGetAliases().setIndices(index).setAliases(alias1).execute(wrap(res -> {
            assertTrue(res.getAliases().size() == 1);
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            client.admin().indices().prepareAliases().addAlias(index, alias2).execute().actionGet();
            GetAliasesResponse getAliasesResponse =
                    client.admin().indices().prepareGetAliases().setIndices(index).setAliases(alias1).execute().actionGet();
            assertTrue(getAliasesResponse.getAliases().size() == 1);
        }
    }

    @Test
    void test_synced_flush() throws Exception {
        final String index = "test_synced_flush";
        CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate(index).execute().actionGet();

        client.admin().indices().prepareSyncedFlush(index).execute(wrap(res -> {
            assertEquals(res.restStatus(), RestStatus.OK);
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            SyncedFlushResponse syncedFlushResponse = client.admin().indices().prepareSyncedFlush(index).execute().actionGet();
            assertEquals(syncedFlushResponse.restStatus(), RestStatus.OK);
        }
    }

    @Test
    void test_get_field_mappings() throws Exception {
        final String index = "test_get_field_mappings";
        final String type = "test_type";
        final String id = "0";
        final String field = "content";
        CountDownLatch latch = new CountDownLatch(1);
        client.prepareIndex(index, type, id)
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .setSource(
                        "{" + "\"user\":\"user_" + id + "\"," + "\"postDate\":\"2018-07-30\"," + "\"" + field + "\": \"elasticsearch\""
                                + "}", XContentType.JSON).execute().actionGet();
        client.admin().indices().prepareRefresh(index).execute().actionGet();

        client.admin().indices().prepareGetFieldMappings().setIndices(index).setTypes(type).setFields(field).execute(wrap(res -> {
            assertTrue(res.mappings().size() > 0);
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            GetFieldMappingsResponse getFieldMappingsResponse =
                    client.admin().indices().prepareGetFieldMappings().setIndices(index).setTypes(type).setFields(field).execute()
                            .actionGet();
            assertTrue(getFieldMappingsResponse.mappings().size() > 0);
        }
    }

    // TODO:  [ERROR]org.elasticsearch.ElasticsearchException: Indices are not found: 400
    void test_info() throws Exception {
        {
            MainResponse mainResponse = client.execute(MainAction.INSTANCE, new MainRequest()).actionGet();
            assertTrue(mainResponse.isAvailable());
        }
    }
}
