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
import org.elasticsearch.action.explain.ExplainAction;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpExplainAction extends HttpAction {

    protected final ExplainAction action;

    public HttpExplainAction(final HttpClient client, final ExplainAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final ExplainRequest request,
            final ActionListener<ExplainResponse> listener) {
        String source = null;
        try {
            final XContentBuilder builder =
                    XContentFactory.jsonBuilder().startObject().field(QUERY_FIELD.getPreferredName(), request.query()).endObject();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to parse a request.", e);
        }
        client.getCurlRequest(POST, "/" + request.type() + "/" + request.id() + "/_explain", request.index()).param("routing", request.routing())
                .param("preference", request.preference()).body(source).execute(response -> {
            try (final InputStream in = response.getContentAsStream()) {
                if (response.getHttpStatusCode() != 200) {
                    throw new ElasticsearchException("error: " + response.getHttpStatusCode());
                }
                final XContentParser parser = createParser(in);
                final ExplainResponse explainResponse = getExplainResponse(parser);
                listener.onResponse(explainResponse);
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);
    }

    protected ExplainResponse getExplainResponse(final XContentParser parser) {
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
}
