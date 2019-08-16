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
package org.codelibs.elasticsearch.client.action;

import java.io.IOException;
import java.util.Locale;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.codelibs.elasticsearch.client.util.UrlUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.VersionType;

public class HttpUpdateAction extends HttpAction {

    protected final UpdateAction action;

    public HttpUpdateAction(final HttpClient client, final UpdateAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final UpdateRequest request, final ActionListener<UpdateResponse> listener) {
        String source = null;
        try (final XContentBuilder builder = request.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)) {
            builder.flush();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to parse a request.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final UpdateResponse updateResponse = UpdateResponse.fromXContent(parser);
                listener.onResponse(updateResponse);
            } catch (final Throwable t) {
                listener.onFailure(toElasticsearchException(response, t));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final UpdateRequest request) {
        // RestUpdateAction
        final CurlRequest curlRequest =
                client.getCurlRequest(POST, "/" + UrlUtils.encode(request.type()) + "/" + UrlUtils.encode(request.id()) + "/_update",
                        request.index());
        if (request.routing() != null) {
            curlRequest.param("routing", request.routing());
        }
        if (request.parent() != null) {
            curlRequest.param("parent", request.parent());
        }
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        if (!RefreshPolicy.NONE.equals(request.getRefreshPolicy())) {
            curlRequest.param("refresh", request.getRefreshPolicy().getValue());
        }
        if (!ActiveShardCount.DEFAULT.equals(request.waitForActiveShards())) {
            curlRequest.param("wait_for_active_shards", String.valueOf(getActiveShardsCountValue(request.waitForActiveShards())));
        }
        curlRequest.param("doc_as_upsert", Boolean.toString(request.docAsUpsert()));
        curlRequest.param("retry_on_conflict", String.valueOf(request.retryOnConflict()));
        curlRequest.param("if_seq_no", Long.toString(request.ifSeqNo()));
        curlRequest.param("if_primary_term", Long.toString(request.ifPrimaryTerm()));
        if (request.version() >= 0) {
            curlRequest.param("version", Long.toString(request.version()));
        }
        if (!VersionType.INTERNAL.equals(request.versionType())) {
            curlRequest.param("version_type", request.versionType().name().toLowerCase(Locale.ROOT));
        }
        return curlRequest;
    }
}
