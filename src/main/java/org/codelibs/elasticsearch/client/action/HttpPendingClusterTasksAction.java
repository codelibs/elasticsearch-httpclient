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
import java.io.UncheckedIOException;
import java.util.List;

import org.codelibs.curl.CurlRequest;
import org.codelibs.elasticsearch.client.HttpClient;
import org.codelibs.elasticsearch.client.io.stream.ByteArrayStreamOutput;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksAction;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksRequest;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.elasticsearch.cluster.service.PendingClusterTask;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

public class HttpPendingClusterTasksAction extends HttpAction {

    protected final PendingClusterTasksAction action;

    public HttpPendingClusterTasksAction(final HttpClient client, final PendingClusterTasksAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final PendingClusterTasksRequest request, final ActionListener<PendingClusterTasksResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final PendingClusterTasksResponse pendingClusterTasksResponse = getPendingClusterTasksResponse(parser);
                listener.onResponse(pendingClusterTasksResponse);
            } catch (final Exception e) {
                listener.onFailure(toElasticsearchException(response, e));
            }
        }, e -> unwrapElasticsearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final PendingClusterTasksRequest request) {
        // RestPendingClusterTasksAction
        final CurlRequest curlRequest = client.getCurlRequest(GET, "/_cluster/pending_tasks");
        curlRequest.param("local", Boolean.toString(request.local()));
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }

    protected PendingClusterTasksResponse getPendingClusterTasksResponse(final XContentParser parser) {
        @SuppressWarnings("unchecked")
        final ConstructingObjectParser<PendingClusterTasksResponse, Void> objectParser =
                new ConstructingObjectParser<>("pending_cluster_tasks", true, a -> {
                    try (final ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
                        final List<PendingClusterTask> pendingClusterTasks = (a[0] != null ? (List<PendingClusterTask>) a[0] : null);

                        out.writeVInt(pendingClusterTasks.size());
                        for (final PendingClusterTask task : pendingClusterTasks) {
                            task.writeTo(out);
                        }

                        return action.getResponseReader().read(out.toStreamInput());
                    } catch (final IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });

        objectParser.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(), getPendingClusterTaskParser(), TASKS_FIELD);

        return objectParser.apply(parser, null);
    }

    protected ConstructingObjectParser<PendingClusterTask, Void> getPendingClusterTaskParser() {
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
}
