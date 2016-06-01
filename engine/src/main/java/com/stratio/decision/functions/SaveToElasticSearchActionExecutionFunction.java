/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.decision.functions;

import com.stratio.decision.commons.messages.ColumnNameTypeValue;
import com.stratio.decision.commons.messages.StratioStreamingMessage;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;

public class SaveToElasticSearchActionExecutionFunction extends BaseActionExecutionFunction {

    private static final long serialVersionUID = 3522740757019463301L;

    private static final Logger log = LoggerFactory.getLogger(SaveToElasticSearchActionExecutionFunction.class);

    private static final SimpleDateFormat elasicSearchTimestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");

    private transient Client elasticSearchClient;

    private final List<String> elasticSearchHosts;
    private final String elasticSearchClusterName;
    private final Integer maxBatchSize;

    private static final String INDEX_NAME = "stratiodecision";

    public SaveToElasticSearchActionExecutionFunction(List<String> elasticSearchHosts, String
            elasticSearchClusterName, Integer maxBatchSize) {
        this.elasticSearchHosts = elasticSearchHosts;
        this.elasticSearchClusterName = elasticSearchClusterName;
        this.maxBatchSize = maxBatchSize==null?1000:maxBatchSize;
    }

    public SaveToElasticSearchActionExecutionFunction(List<String> elasticSearchHosts, String
            elasticSearchClusterName, Integer maxBatchSize, Client elasticSearchClient) {
        this(elasticSearchHosts, elasticSearchClusterName, maxBatchSize);
        this.elasticSearchClient = elasticSearchClient;
    }

    @Override
    public Boolean check() throws Exception {
        try {
            getClient().admin().indices().prepareExists(elasticSearchClusterName).execute();
            return true;
        } catch (Exception e) {
            return false;
        }
    }



    @Override
    public void process(Iterable<StratioStreamingMessage> messages) throws Exception {
        try {


             BulkProcessor bulkProcessor = BulkProcessor.builder(getClient(), new BulkProcessor.Listener(){
                 @Override
                 public void beforeBulk(long executionId, BulkRequest request) {
                     log.debug("Going to execute new elastic search bulk composed of {} actions",  request
                             .numberOfActions());
                 }

                 @Override
                 public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                     log.debug("Executed elastic search bulk composed of {} actions", request.numberOfActions());
                 }

                 @Override
                 public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                     log.error("Error executing elastic search bulk: {}", failure);
                 }
             }).setBulkActions(maxBatchSize).build();


            for (StratioStreamingMessage stratioStreamingMessage : messages) {
                try {

                    XContentBuilder contentBuilder = XContentFactory.jsonBuilder().startObject();

                    for (ColumnNameTypeValue column : stratioStreamingMessage.getColumns()) {
                        contentBuilder = contentBuilder.field(column.getColumn(), column.getValue());
                    }
                    contentBuilder = contentBuilder.field("@timestamp",
                            elasicSearchTimestampFormat.format(stratioStreamingMessage.getTimestamp()));

                    contentBuilder = contentBuilder.endObject();
                    IndexRequestBuilder request = getClient().prepareIndex(INDEX_NAME,
                            stratioStreamingMessage.getStreamName()).setSource(contentBuilder);


                    bulkProcessor.add(request.request());

                } catch (IOException e) {
                    log.error("Error generating a index to event element into stream {}",
                            stratioStreamingMessage.getStreamName(), e);
                }

            }

            bulkProcessor.close();


        } catch (Exception e) {
            log.error("Error in ElasticSearch: " + e.getMessage());
        }

    }

    private Client getClient() {
        if (elasticSearchClient == null) {

            elasticSearchClient = (Client) ActionBaseContext.getInstance().getContext().getBean
                    ("elasticsearchClient");
        }
        return elasticSearchClient;
    }
}
