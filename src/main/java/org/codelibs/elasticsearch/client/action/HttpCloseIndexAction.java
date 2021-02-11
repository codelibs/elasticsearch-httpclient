/*
 * Copyright 2012-2021 CodeLibs Project and the Others.
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
package org.codelibs.elasticsearch.client.action;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse.IndexResult;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse.ShardResult;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse.ShardResult.Failure;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;

public class HttpCloseIndexAction extends HttpAction {

    protected final CloseIndexAction action;

    public HttpCloseIndexAction(final HttpClient client, final CloseIndexAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final CloseIndexRequest request, final ActionListener<CloseIndexResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final CloseIndexResponse closeIndexResponse = fromXContent(parser);
                listener.onResponse(closeIndexResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final CloseIndexRequest request) {
        // RestCloseIndexAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_close", request.indices());
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }

    protected CloseIndexResponse fromXContent(final XContentParser parser) throws IOException {
        List<IndexResult> indices = Collections.emptyList();
        boolean shardsAcknowledged = false;
        boolean acknowledged = false;
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("indices".equals(fieldName)) {
                    parser.nextToken();
                    indices = parseIndices(parser);
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if ("shards_acknowledged".equals(fieldName)) {
                    shardsAcknowledged = parser.booleanValue();
                } else if ("acknowledged".equals(fieldName)) {
                    acknowledged = parser.booleanValue();
                }
            }
            parser.nextToken();
        }
        return new CloseIndexResponse(acknowledged, shardsAcknowledged, indices);
    }

    protected List<IndexResult> parseIndices(final XContentParser parser) throws IOException {
        final List<IndexResult> list = new ArrayList<>();
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                parser.nextToken();
                list.add(parseIndexResult(parser, fieldName));
            }
            parser.nextToken();
        }
        return list;
    }

    protected IndexResult parseIndexResult(final XContentParser parser, final String index) throws IOException {
        boolean closed = false;
        ElasticsearchException eex = null;
        final List<ShardResult> shardResults = new ArrayList<>();
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("exception".equals(fieldName)) {
                    parser.nextToken();
                    eex = ElasticsearchException.fromXContent(parser);
                } else if ("failedShards".equals(fieldName)) {
                    parser.nextToken();
                    shardResults.addAll(parseShardResults(parser));
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if ("closed".equals(fieldName)) {
                    closed = parser.booleanValue();
                }
            }
            parser.nextToken();
        }
        final Index idx = new Index(index, "unknown");
        if (closed) {
            return new IndexResult(idx);
        }
        if (eex != null) {
            return new IndexResult(idx, eex);
        }
        if (!shardResults.isEmpty()) {
            return new IndexResult(idx, shardResults.toArray(new ShardResult[shardResults.size()]));
        }
        return new IndexResult(idx);
    }

    protected List<ShardResult> parseShardResults(final XContentParser parser) throws IOException {
        final List<ShardResult> list = new ArrayList<>();
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                parser.nextToken();
                list.add(parseShardResult(parser, Integer.parseInt(fieldName)));
            }
            parser.nextToken();
        }
        return list;
    }

    protected ShardResult parseShardResult(final XContentParser parser, final int id) throws IOException {
        final List<Failure> failures = new ArrayList<>();
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("failures".equals(fieldName)) {
                    parser.nextToken();
                    while ((token = parser.currentToken()) != XContentParser.Token.END_ARRAY) {
                        failures.add(parseFailure(parser));
                    }
                }
            }
            parser.nextToken();
        }
        return new ShardResult(id, failures.toArray(new Failure[failures.size()]));
    }

    protected Failure parseFailure(final XContentParser parser) throws IOException {
        int shardId = -1;
        String index = null;
        ElasticsearchException eex = null;
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("reason".equals(fieldName)) {
                    parser.nextToken();
                    eex = ElasticsearchException.fromXContent(parser);
                    parser.nextToken();
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("index".equals(fieldName)) {
                    index = parser.text();
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("shard".equals(fieldName)) {
                    shardId = parser.intValue();
                }
                // TODO status
            }
            parser.nextToken();
        }
        return new Failure(index, shardId, eex);
    }
}
