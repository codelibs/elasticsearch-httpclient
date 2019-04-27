package org.codelibs.elasticsearch.client.action;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNode.Role;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.discovery.DiscoveryStats;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.ingest.IngestStats;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.monitor.process.ProcessStats;
import org.elasticsearch.node.AdaptiveSelectionStats;
import org.elasticsearch.script.ScriptStats;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.transport.TransportStats;

public class HttpNodesStatsAction extends HttpAction {

    protected NodesStatsAction action;

    public HttpNodesStatsAction(HttpClient client, NodesStatsAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final NodesStatsRequest request, final ActionListener<NodesStatsResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final NodesStatsResponse nodesStatsResponse = fromXContent(parser);
                listener.onResponse(nodesStatsResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected NodesStatsResponse fromXContent(XContentParser parser) throws IOException {
        List<NodeStats> nodes = Collections.emptyList();
        String fieldName = null;
        int[] nodeResults = new int[3];
        ClusterName clusterName = ClusterName.DEFAULT;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("_nodes".equals(fieldName)) {
                    nodeResults = parseNodeResults(parser);
                } else if ("nodes".equals(fieldName)) {
                    parser.nextToken();
                    nodes = parseNodes(parser);
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (fieldName.equals("cluster_name")) {
                    clusterName = new ClusterName(parser.text());
                }
            }
            parser.nextToken();
        }
        return new NodesStatsResponse(clusterName, nodes, Collections.emptyList());
    }

    private List<NodeStats> parseNodes(XContentParser parser) throws IOException {
        List<NodeStats> list = new ArrayList<>();
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                parser.nextToken();
                list.add(parseNodeStats(parser, fieldName));
            }
            parser.nextToken();
        }
        return list;
    }

    private NodeStats parseNodeStats(XContentParser parser, String nodeId) throws IOException {
        List<NodeStats> list = new ArrayList<>();
        String fieldName = null;
        String nodeName = "";
        long timestamp = 0;
        Set<Role> roles = new HashSet<>();
        NodeIndicesStats indices = null;
        OsStats os = null;
        ProcessStats process = null;
        JvmStats jvm = null;
        ThreadPoolStats threadPool = null;
        FsInfo fs = null;
        TransportStats transport = null;
        HttpStats http = null;
        AllCircuitBreakerStats breaker = null;
        ScriptStats scriptStats = null;
        DiscoveryStats discoveryStats = null;
        IngestStats ingestStats = null;
        AdaptiveSelectionStats adaptiveSelectionStats = null;
        XContentParser.Token token;
        TransportAddress transportAddress = new TransportAddress(TransportAddress.META_ADDRESS, 0);
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("indices".equals(fieldName)) {
                    parser.nextToken();
                    indices = parseNodeIndicesStats(parser);
                } else if ("os".equals(fieldName)) {
                    parser.nextToken();
                    os = parseOsStats(parser);
                } else if ("process".equals(fieldName)) {
                    parser.nextToken();
                    process = parseProcessStats(parser);
                } else if ("jvm".equals(fieldName)) {
                    parser.nextToken();
                    jvm = parseJvmStats(parser);
                } else if ("thread_pool".equals(fieldName)) {
                    parser.nextToken();
                    threadPool = parseThreadPoolStats(parser);
                } else if ("fs".equals(fieldName)) {
                    parser.nextToken();
                    fs = parseFsInfo(parser);
                } else if ("transport".equals(fieldName)) {
                    parser.nextToken();
                    transport = parseTransportStats(parser);
                } else if ("http".equals(fieldName)) {
                    parser.nextToken();
                    http = parseHttpStats(parser);
                } else if ("breakers".equals(fieldName)) {
                    parser.nextToken();
                    breaker = parseAllCircuitBreakerStats(parser);
                } else if ("script".equals(fieldName)) {
                    parser.nextToken();
                    scriptStats = parseScriptStats(parser);
                } else if ("discovery".equals(fieldName)) {
                    parser.nextToken();
                    discoveryStats = parseDiscoveryStats(parser);
                } else if ("ingest".equals(fieldName)) {
                    parser.nextToken();
                    ingestStats = parseIngestStats(parser);
                } else if ("adaptive_selection".equals(fieldName)) {
                    parser.nextToken();
                    adaptiveSelectionStats = parseAdaptiveSelectionStats(parser);
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("timestamp".equals(fieldName)) {
                    timestamp = parser.longValue();
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("name".equals(fieldName)) {
                    nodeName = parser.text();
                } else if ("transport_address".equals(fieldName)) {
                    // TODO parse ip:port   
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("roles".equals(fieldName)) {
                    while ((token = parser.currentToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            String role = parser.text();
                            if (Role.MASTER.getRoleName().equals(role)) {
                                roles.add(Role.MASTER);
                            } else if (Role.DATA.getRoleName().equals(role)) {
                                roles.add(Role.DATA);
                            } else if (Role.INGEST.getRoleName().equals(role)) {
                                roles.add(Role.INGEST);
                            }
                        }
                        parser.nextToken();
                    }

                }
            }
            parser.nextToken();
        }

        DiscoveryNode node = new DiscoveryNode(nodeName, nodeId, transportAddress, Collections.emptyMap(), roles, Version.CURRENT);
        return new NodeStats(node, timestamp, indices, os, process, jvm, threadPool, fs, transport, http, breaker, scriptStats,
                discoveryStats, ingestStats, adaptiveSelectionStats);
    }

    private AdaptiveSelectionStats parseAdaptiveSelectionStats(XContentParser parser) throws IOException {
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            // TODO
            parser.nextToken();
        }
        return null;
    }

    private IngestStats parseIngestStats(XContentParser parser) throws IOException {
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            // TODO
            parser.nextToken();
        }
        return null;
    }

    private DiscoveryStats parseDiscoveryStats(XContentParser parser) throws IOException {
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            // TODO
            parser.nextToken();
        }
        return null;
    }

    private ScriptStats parseScriptStats(XContentParser parser) throws IOException {
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            // TODO
            parser.nextToken();
        }
        return null;
    }

    private AllCircuitBreakerStats parseAllCircuitBreakerStats(XContentParser parser) throws IOException {
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            // TODO
            parser.nextToken();
        }
        return null;
    }

    private HttpStats parseHttpStats(XContentParser parser) throws IOException {
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            // TODO
            parser.nextToken();
        }
        return null;
    }

    private TransportStats parseTransportStats(XContentParser parser) throws IOException {
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            // TODO
            parser.nextToken();
        }
        return null;
    }

    private FsInfo parseFsInfo(XContentParser parser) throws IOException {
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            // TODO
            parser.nextToken();
        }
        return null;
    }

    private ThreadPoolStats parseThreadPoolStats(XContentParser parser) throws IOException {
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            // TODO
            parser.nextToken();
        }
        return null;
    }

    private JvmStats parseJvmStats(XContentParser parser) throws IOException {
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            // TODO
            parser.nextToken();
        }
        return null;
    }

    private ProcessStats parseProcessStats(XContentParser parser) throws IOException {
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            // TODO
            parser.nextToken();
        }
        return null;
    }

    private OsStats parseOsStats(XContentParser parser) throws IOException {
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            // TODO
            parser.nextToken();
        }
        return null;
    }

    private NodeIndicesStats parseNodeIndicesStats(XContentParser parser) throws IOException {
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            // TODO
            parser.nextToken();
        }
        return null;
    }

    private int[] parseNodeResults(XContentParser parser) throws IOException {
        int results[] = new int[3];
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("total".equals(fieldName)) {
                    results[0] = parser.intValue();
                } else if ("successful".equals(fieldName)) {
                    results[1] = parser.intValue();
                } else if ("failed".equals(fieldName)) {
                    results[2] = parser.intValue();
                }
            }
            parser.nextToken();
        }
        return results;
    }

    protected String getMetric(NodesStatsRequest request) {
        List<String> list = new ArrayList<>();
        if (request.os()) {
            list.add("os");
        }
        if (request.jvm()) {
            list.add("jvm");
        }
        if (request.threadPool()) {
            list.add("thread_pool");
        }
        if (request.fs()) {
            list.add("fs");
        }
        if (request.transport()) {
            list.add("transport");
        }
        if (request.http()) {
            list.add("http");
        }
        if (request.process()) {
            list.add("process");
        }
        if (request.breaker()) {
            list.add("breaker");
        }
        if (request.script()) {
            list.add("script");
        }
        if (request.discovery()) {
            list.add("discovery");
        }
        if (request.ingest()) {
            list.add("ingest");
        }
        if (request.adaptiveSelection()) {
            list.add("adaptive_selection");
        }
        if (request.indices().anySet() && CommonStatsFlags.ALL.getFlags().length != request.indices().getFlags().length) {
            list.add("indices");
            return list.stream().collect(Collectors.joining(",")) + "/"
                    + Arrays.stream(request.indices().getFlags()).map(f -> f.getRestName()).collect(Collectors.joining(","));
        } else {
            return list.stream().collect(Collectors.joining(","));
        }
    }

    protected CurlRequest getCurlRequest(final NodesStatsRequest request) {
        // RestNodesStatsAction
        StringBuilder buf = new StringBuilder();
        buf.append("/_nodes");
        if (request.nodesIds() != null && request.nodesIds().length > 0) {
            buf.append('/').append(String.join(",", request.nodesIds()));
        }
        buf.append("/stats");
        String metric = getMetric(request);
        if (metric.length() > 0) {
            buf.append('/').append(metric);
        }
        final CurlRequest curlRequest = client.getCurlRequest(GET, buf.toString());
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        return curlRequest;
    }
}
