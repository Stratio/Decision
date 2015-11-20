/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.decision.functions;

import com.stratio.decision.commons.messages.ColumnNameTypeValue;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
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

    private Client elasticSearchClient;

    private final List<String> elasticSearchHosts;
    private final String elasticSearchClusterName;

    public SaveToElasticSearchActionExecutionFunction(List<String> elasticSearchHosts, String elasticSearchClusterName) {
        this.elasticSearchHosts = elasticSearchHosts;
        this.elasticSearchClusterName = elasticSearchClusterName;
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

            BulkRequestBuilder bulkBuilder = getClient().prepareBulk();
            for (StratioStreamingMessage stratioStreamingMessage : messages) {
                try {
                    XContentBuilder contentBuilder = XContentFactory.jsonBuilder().startObject();

                    for (ColumnNameTypeValue column : stratioStreamingMessage.getColumns()) {
                        contentBuilder = contentBuilder.field(column.getColumn(), column.getValue());
                    }
                    contentBuilder = contentBuilder.field("@timestamp",
                            elasicSearchTimestampFormat.format(stratioStreamingMessage.getTimestamp()));

                    contentBuilder = contentBuilder.endObject();
                    IndexRequestBuilder request = getClient().prepareIndex("stratiostreaming",
                            stratioStreamingMessage.getStreamName()).setSource(contentBuilder);
                    bulkBuilder.add(request);

                } catch (IOException e) {
                    log.error("Error generating a index to event element into stream {}",
                            stratioStreamingMessage.getStreamName(), e);
                }

            }

            bulkBuilder.execute().actionGet();

        } catch (Exception e) {
            log.error("Error in ElasticSearch: " + e.getMessage());
        }

    }

    private Client getClient() {
        if (elasticSearchClient == null) {
            Settings settings = ImmutableSettings.settingsBuilder()
                    .put("client.transport.ignore_cluster_name", true)
                    .put("cluster.name", elasticSearchClusterName)
                    .build();
            TransportClient tc = new TransportClient(settings);
            for (String elasticSearchHost : elasticSearchHosts) {
                String[] elements = elasticSearchHost.split(":");
                tc.addTransportAddress(new InetSocketTransportAddress(elements[0], Integer.parseInt(elements[1])));
            }
            elasticSearchClient = tc;
        }
        return elasticSearchClient;
    }
}
