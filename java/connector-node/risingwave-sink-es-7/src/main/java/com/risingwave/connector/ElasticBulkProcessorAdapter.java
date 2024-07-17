/*
 * Copyright 2024 RisingWave Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.risingwave.connector;

import com.risingwave.connector.EsSink.RequestTracker;
import java.util.concurrent.TimeUnit;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticBulkProcessorAdapter implements BulkProcessorAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(EsSink.class);
    BulkProcessor esBulkProcessor;
    private final RequestTracker requestTracker;

    public ElasticBulkProcessorAdapter(
            RequestTracker requestTracker, ElasticRestHighLevelClientAdapter client) {
        BulkProcessor.Builder builder =
                BulkProcessor.builder(
                        (bulkRequest, bulkResponseActionListener) ->
                                client.bulkAsync(
                                        bulkRequest,
                                        RequestOptions.DEFAULT,
                                        bulkResponseActionListener),
                        new BulkListener(requestTracker));
        // Possible feature: move these to config
        // execute the bulk every 10 000 requests
        builder.setBulkActions(1000);
        // flush the bulk every 5mb
        builder.setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB));
        // flush the bulk every 5 seconds whatever the number of requests
        builder.setFlushInterval(TimeValue.timeValueSeconds(5));
        // Set the number of concurrent requests
        builder.setConcurrentRequests(1);
        // Set a custom backoff policy which will initially wait for 100ms, increase exponentially
        // and retries up to three times.
        builder.setBackoffPolicy(
                BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3));
        this.esBulkProcessor = builder.build();
        this.requestTracker = requestTracker;
    }

    @Override
    public void flush() {
        esBulkProcessor.flush();
    }

    @Override
    public void awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
        esBulkProcessor.awaitClose(timeout, unit);
    }

    @Override
    public void addRow(String index, String key, String doc) throws InterruptedException {
        UpdateRequest updateRequest;
        updateRequest = new UpdateRequest(index, "_doc", key).doc(doc, XContentType.JSON);
        updateRequest.docAsUpsert(true);
        this.requestTracker.addWriteTask();
        this.esBulkProcessor.add(updateRequest);
    }

    @Override
    public void deleteRow(String index, String key) throws InterruptedException {
        DeleteRequest deleteRequest;
        deleteRequest = new DeleteRequest(index, "_doc", key);
        this.requestTracker.addWriteTask();
        this.esBulkProcessor.add(deleteRequest);
    }
}
