package com.stratio.streaming.functions;

import java.io.IOException;
import java.text.SimpleDateFormat;

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

import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;

public class SaveToElasticSearchActionExecutionFunction extends BaseActionExecutionFunction {

    private static final long serialVersionUID = 3522740757019463301L;

    private static final Logger log = LoggerFactory.getLogger(SaveToElasticSearchActionExecutionFunction.class);

    private static final SimpleDateFormat elasicSearchTimestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");

    private Client elasticSearchClient;

    private final String elasticSearchHost;
    private final int elasticSearchPort;

    public SaveToElasticSearchActionExecutionFunction(String elasticSearchHost, int elasticSearchPort) {
        this.elasticSearchHost = elasticSearchHost;
        this.elasticSearchPort = elasticSearchPort;
    }

    @Override
    public void process(Iterable<StratioStreamingMessage> messages) throws Exception {

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

    }

    private Client getClient() {
        if (elasticSearchClient == null) {
            Settings settings = ImmutableSettings.settingsBuilder().put("client.transport.ignore_cluster_name", true)
                    .build();
            elasticSearchClient = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(
                    elasticSearchHost, elasticSearchPort));
        }
        return elasticSearchClient;
    }
}
