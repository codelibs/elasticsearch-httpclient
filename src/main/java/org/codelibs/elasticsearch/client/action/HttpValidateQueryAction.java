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
package org.codelibs.elasticsearch.client.action;

import java.io.InputStream;

import org.codelibs.elasticsearch.client.HttpClient;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class ValidateQueryAction extends HttpAction {

    protected final ValidateQueryAction action;

    public HttpValidateQueryAction(final HttpClient client, final ValidateQueryAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final ValidateQueryRequest request,
            final ActionListener<ValidateQueryResponse> listener) {
        String source = null;
        try {
            final XContentBuilder builder =
                    XContentFactory.jsonBuilder().startObject().field(QUERY_FIELD.getPreferredName(), request.query()).endObject();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to parse a request.", e);
        }
        client.getCurlRequest(GET, (request.types() == null ? "" : "/" + String.join(",", request.types())) + "/_validate/query",
                request.indices()).param("explain", String.valueOf(request.explain())).param("rewrite", String.valueOf(request.rewrite()))
                .param("all_shards", String.valueOf(request.allShards())).body(source).execute(response -> {
            if (response.getHttpStatusCode() != 200) {
                throw new ElasticsearchException("error: " + response.getHttpStatusCode());
            }
            try (final InputStream in = response.getContentAsStream()) {
                final XContentParser parser = createParser(in);
                final ValidateQueryResponse validateQueryResponse = getValidateQueryResponse(parser, action::newResponse);
                listener.onResponse(validateQueryResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }

    protected ValidateQueryResponse getValidateQueryResponse(final XContentParser parser, final Supplier<ValidateQueryResponse> newResponse)
            throws IOException {
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
}
