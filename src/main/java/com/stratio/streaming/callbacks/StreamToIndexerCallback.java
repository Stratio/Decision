package com.stratio.streaming.callbacks;

import java.io.IOException;
import java.util.List;

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
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import com.google.common.collect.Lists;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;

public class StreamToIndexerCallback extends StreamCallback implements MessageListener<String> {

    private static Logger logger = LoggerFactory.getLogger(StreamToCassandraCallback.class);

    private final StreamDefinition streamDefinition;
    private final Client elasticSearchClient;

    public StreamToIndexerCallback(StreamDefinition streamDefinition, String elasticSearchHost, int elasticSearchPort) {
        this.streamDefinition = streamDefinition;
        Settings settings = ImmutableSettings.settingsBuilder().put("client.transport.ignore_cluster_name", true)
                .build();
        this.elasticSearchClient = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(
                elasticSearchHost, elasticSearchPort));
    }

    @Override
    public void onMessage(Message<String> message) {
        logger.debug("New on message {}", message);

    }

    @Override
    public void receive(Event[] events) {
        List<StratioStreamingMessage> collected_events = Lists.newArrayList();
        for (Event e : events) {
            if (e instanceof InEvent) {
                InEvent ie = (InEvent) e;
                List<ColumnNameTypeValue> columns = Lists.newArrayList();
                for (Attribute column : streamDefinition.getAttributeList()) {

                    // avoid retrieving a value out of the scope
                    // outputStream could have more fields defined than the
                    // output events (projection)
                    if (ie.getData().length >= streamDefinition.getAttributePosition(column.getName()) + 1) {
                        columns.add(new ColumnNameTypeValue(column.getName(), column.getType().toString(), ie
                                .getData(streamDefinition.getAttributePosition(column.getName()))));
                    }
                }
                collected_events.add(new StratioStreamingMessage(streamDefinition.getStreamId(), // value.streamName
                        ie.getTimeStamp(), // value.timestamp
                        columns)); // value.columns
            }
        }

        BulkRequestBuilder bulkBuilder = elasticSearchClient.prepareBulk();
        for (StratioStreamingMessage stratioStreamingMessage : collected_events) {
            try {
                XContentBuilder contentBuilder = XContentFactory.jsonBuilder().startObject();

                for (ColumnNameTypeValue column : stratioStreamingMessage.getColumns()) {
                    contentBuilder = contentBuilder.field(column.getColumn(), column.getValue());
                }
                contentBuilder = contentBuilder.endObject();
                IndexRequestBuilder request = elasticSearchClient.prepareIndex("stratiostreaming",
                        stratioStreamingMessage.getStreamName()).setSource(contentBuilder);
                bulkBuilder.add(request);

            } catch (IOException e) {
                logger.error("Error generating a index to event element into stream {}",
                        stratioStreamingMessage.getStreamName(), e);
            }

        }

        bulkBuilder.execute().actionGet();
    }
}
