package org.codelibs.elasticsearch.client;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.ClearScrollRequestBuilder;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class HttpClientTest {

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
    void test_refresh() throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate("refresh").execute().actionGet();
        client.admin().indices().prepareRefresh("refresh").execute(wrap(res -> {
            assertEquals(res.getStatus(), RestStatus.OK);
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            RefreshResponse res = client.admin().indices().prepareRefresh("refresh").execute().actionGet();
            assertEquals(res.getStatus(), RestStatus.OK);
        }

    }

    @Test
    void test_search() throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(1);
        client.prepareSearch().setQuery(QueryBuilders.matchAllQuery()).execute(wrap(res -> {
            assertEquals(0, res.getHits().getTotalHits());
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            SearchResponse res = client.prepareSearch().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
            assertEquals(0, res.getHits().getTotalHits());
        }

    }

    @Test
    void test_create_index() throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate("create_index1").execute(wrap(res -> {
            assertTrue(res.isAcknowledged());
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            CreateIndexResponse res = client.admin().indices().prepareCreate("create_index2").execute().actionGet();
            assertTrue(res.isAcknowledged());
        }

    }

    @Test
    void test_delete_index() throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate("delete_index1").execute().actionGet();
        client.admin().indices().prepareDelete("delete_index1").execute(wrap(res -> {
            assertTrue(res.isAcknowledged());
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            client.admin().indices().prepareCreate("delete_index2").execute().actionGet();
            DeleteIndexResponse res = client.admin().indices().prepareDelete("delete_index2").execute().actionGet();
            assertTrue(res.isAcknowledged());
        }

    }

    @Test
    void test_get_index() throws Exception {
        String index = "get_index";
        final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()//
                .startObject()//
                .startObject("properties")//
                .startObject("test_prop")//
                .field("type", "text")//
                .endObject()//
                .endObject()//
                .endObject();
        String source = mappingBuilder.string();
        CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate(index).execute().actionGet();
        client.admin().indices().prepareAliases().addAlias(index, "test_alias").execute().actionGet();
        client.admin().indices().preparePutMapping(index).setType("test_type").setSource(source, XContentType.JSON).execute().actionGet();
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
            GetIndexResponse res = client.admin().indices().prepareGetIndex().addIndices(index).execute().actionGet();
            assertEquals(index, res.getIndices()[0]);
            assertTrue(res.getAliases().containsKey(index));
            assertTrue(res.getMappings().containsKey(index));
            assertTrue(res.getSettings().containsKey(index));
        }

    }

    @Test
    void test_open_index() throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate("open_index1").execute().actionGet();
        client.admin().indices().prepareOpen("open_index1").execute(wrap(res -> {
            assertTrue(res.isAcknowledged());
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            client.admin().indices().prepareCreate("open_index2").execute().actionGet();
            OpenIndexResponse res = client.admin().indices().prepareOpen("open_index2").execute().actionGet();
            assertTrue(res.isAcknowledged());
        }

    }

    @Test
    void test_close_index() throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate("close_index1").execute().actionGet();
        client.admin().indices().prepareClose("close_index1").execute(wrap(res -> {
            assertTrue(res.isAcknowledged());
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            client.admin().indices().prepareCreate("close_index2").execute().actionGet();
            CloseIndexResponse res = client.admin().indices().prepareClose("close_index2").execute().actionGet();
            assertTrue(res.isAcknowledged());
        }

    }

    @Test
    void test_indices_exists() throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate("indices_exists").execute().actionGet();
        client.admin().indices().prepareExists("indices_exists").execute(wrap(res -> {
            assertTrue(res.isExists());
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            IndicesExistsResponse res = client.admin().indices().prepareExists("indices_exists").execute().actionGet();
            assertTrue(res.isExists());
            res = client.admin().indices().prepareExists("indices_exists_not").execute().actionGet();
            assertTrue(!res.isExists());
        }

    }

    @Test
    void test_indices_aliases() throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate("indices_aliases").execute().actionGet();
        client.admin().indices().prepareAliases().addAlias("indices_aliases", "test_alias1").execute(wrap(res -> {
            assertTrue(res.isAcknowledged());
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        {
            IndicesAliasesResponse res =
                    client.admin().indices().prepareAliases().addAlias("indices_aliases", "test_alias2").execute().actionGet();
            assertTrue(res.isAcknowledged());
        }

    }

    @Test
    void test_put_mapping() throws Exception {
        final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()//
                .startObject()//
                .startObject("properties")//
                .startObject("test_prop")//
                .field("type", "text")//
                .endObject()//
                .endObject()//
                .endObject();
        String source = mappingBuilder.string();
        CountDownLatch latch = new CountDownLatch(1);
        client.admin().indices().prepareCreate("put_mapping1").execute().actionGet();
        client.admin().indices().preparePutMapping("put_mapping1").setType("test_type").setSource(source, XContentType.JSON)
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
            client.admin().indices().prepareCreate("put_mapping2").execute().actionGet();
            PutMappingResponse res =
                    client.admin().indices().preparePutMapping("put_mapping2").setType("test_type").setSource(source, XContentType.JSON)
                            .execute().actionGet();
            assertTrue(res.isAcknowledged());
        }

    }

    @Test
    void test_get_mappings() throws Exception {
        String index = "get_mappings1";
        String type = "test_type";
        final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()//
                .startObject()//
                .startObject("properties")//
                .startObject("test_prop")//
                .field("type", "text")//
                .endObject()//
                .endObject()//
                .endObject();
        String source = mappingBuilder.string();
        Map<String, Object> mappingMap = XContentHelper.convertToMap(mappingBuilder.bytes(), true, XContentType.JSON).v2();
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
            GetMappingsResponse res = client.admin().indices().prepareGetMappings(index).execute().actionGet();
            ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = res.getMappings();
            assertTrue(mappings.containsKey(index));
            assertTrue(mappings.get(index).containsKey(type));
            assertEquals(mappings.get(index).get(type), mappingMetaData);
        }
    }

    @Test
    void test_flush() throws Exception {
        String index = "flush";
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

    // todo
    void test_clear_scroll() throws Exception {
        String id = "";
        CountDownLatch latch = new CountDownLatch(1);
        try {
            client.prepareClearScroll().addScrollId(id).execute(wrap(res -> {
                assertFalse(res.isSucceeded());
                assertEquals(res.status(), RestStatus.OK);
                latch.countDown();
            }, e -> {
                e.printStackTrace();
                assertTrue(false);
                latch.countDown();
            }));
            latch.await();
        } catch (Exception e) {

        }
        {
            ClearScrollResponse res = client.prepareClearScroll().addScrollId(id).execute().actionGet();
            assertEquals(res.status(), RestStatus.OK);
        }
    }

    // TODO :
    void test_scroll() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        // assert ElasticserchException
        SearchResponse scrollResponse =
                client.prepareSearch().setQuery(QueryBuilders.queryStringQuery("")).setScroll(new TimeValue(60000)).setSize(1).execute()
                        .actionGet();

        String id = scrollResponse.getScrollId();
        SearchHit[] hits;
        do {
            hits = scrollResponse.getHits().getHits();
            scrollResponse = client.prepareSearchScroll(id).setScroll(new TimeValue(60000)).execute().actionGet();
        } while (hits.length != 0);

        ClearScrollResponse clearScrollResponse = client.prepareClearScroll().addScrollId(id).execute().actionGet();
        assertFalse(clearScrollResponse.isSucceeded());
    }

    @Test
    void test_multi_search() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        SearchRequestBuilder srb1 = client.prepareSearch().setQuery(QueryBuilders.queryStringQuery("word")).setSize(1);
        SearchRequestBuilder srb2 = client.prepareSearch().setQuery(QueryBuilders.matchQuery("name", "fess")).setSize(1);
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
                e.printStackTrace();
                assertTrue(false);
                latch.countDown();
            }));
            latch.await();
        } catch (Exception e) {

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
    void test_field_caps() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        client.prepareFieldCaps().setFields("rating", "keyword").execute(wrap(res -> {
            // TODO
            latch.countDown();
        }, e -> {
            e.printStackTrace();
            assertTrue(false);
            latch.countDown();
        }));
        latch.await();

        // TODO
        //        FieldCapabilitiesResponse response = client.prepareFieldCaps().setFields("rating").execute().actionGet();
    }
}
