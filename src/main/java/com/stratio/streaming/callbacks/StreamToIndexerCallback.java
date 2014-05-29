package com.stratio.streaming.callbacks;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
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
import com.stratio.streaming.utils.SiddhiUtils;

public class StreamToIndexerCallback extends StreamCallback implements MessageListener<String> {

    private static Logger logger = LoggerFactory.getLogger(StreamToIndexerCallback.class);
    private final SimpleDateFormat elasicSearchTimestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");

    private final StreamDefinition streamDefinition;
    private final Client elasticSearchClient;

    private boolean running;

    public StreamToIndexerCallback(StreamDefinition streamDefinition, String elasticSearchHost, int elasticSearchPort) {
        this.streamDefinition = streamDefinition;
        Settings settings = ImmutableSettings.settingsBuilder().put("client.transport.ignore_cluster_name", true)
                .build();
        this.elasticSearchClient = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(
                elasticSearchHost, elasticSearchPort));
        running = Boolean.TRUE;
    }

    @Override
    public void onMessage(Message<String> message) {
        if (running) {
            if (message.getMessageObject().equalsIgnoreCase(streamDefinition.getStreamId())
                    || message.getMessageObject().equalsIgnoreCase("*")) {

                logger.debug("Shutting down index for stream {}", streamDefinition.getStreamId());
                elasticSearchClient.close();
                running = Boolean.FALSE;
            }
        }
    }

    @Override
    public void receive(Event[] events) {
        if (running) {
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
                            columns.add(new ColumnNameTypeValue(column.getName(), SiddhiUtils.encodeSiddhiType(column
                                    .getType()), ie.getData(streamDefinition.getAttributePosition(column.getName()))));
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
                    // Add timestamp element to original object
                    contentBuilder = contentBuilder.field("@timestamp", elasicSearchTimestampFormat.format(new Date()));

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
}
