package org.codelibs.elasticsearch.client;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
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
            PutMappingResponse res = client.admin().indices().preparePutMapping("put_mapping2").setType("test_type")
                    .setSource(source, XContentType.JSON).execute().actionGet();
            assertTrue(res.isAcknowledged());
        }

    }
}
