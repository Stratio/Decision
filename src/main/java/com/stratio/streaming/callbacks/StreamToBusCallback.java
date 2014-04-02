package com.stratio.streaming.callbacks;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.FilterMessagesByOperationFunction;
import com.stratio.streaming.utils.DataToCollectorUtils;

public class StreamToBusCallback extends StreamCallback implements MessageListener<String> {

	private static Logger logger = LoggerFactory.getLogger(StreamToBusCallback.class);
	
	private StreamDefinition streamDefinition;
	private String kafkaCluster;
	private Producer<String, String> producer;
	
	public StreamToBusCallback(StreamDefinition streamDefinition, String kafkaCluster) {
		this.streamDefinition = streamDefinition;
		this.kafkaCluster = kafkaCluster;
		this.producer = new Producer<String, String>(createProducerConfig());
		logger.debug("Starting listener for stream " + streamDefinition.getStreamId());
	}
	
	@Override
	public void receive(Event[] events) {
		
		List<StratioStreamingMessage> collected_events = Lists.newArrayList();
		
		for (Event e : events) {
			if (e instanceof InEvent) {
				InEvent ie = (InEvent) e;
				
				List<ColumnNameTypeValue> columns = Lists.newArrayList();
				
				for (Attribute column : streamDefinition.getAttributeList()) {
					
					columns.add(new ColumnNameTypeValue(column.getName(),
							 								column.getType().toString(),
							 								ie.getData(streamDefinition.getAttributePosition(column.getName()))));
					
					
				}
				
				
				collected_events.add(new StratioStreamingMessage(streamDefinition.getStreamId(), 	// value.streamName
																			ie.getTimeStamp(), 	//value.timestamp
																			columns)); 	 		// value.columns															
									
			}
		}
		
		sendEventsToBus(collected_events);
		DataToCollectorUtils.sendData(collected_events);
		
	}
	


	private void sendEventsToBus(List<StratioStreamingMessage> collected_events) {
		
		for (StratioStreamingMessage event : collected_events) {
			
			KeyedMessage<String, String> message = new KeyedMessage<String, String>(streamDefinition.getId(), 					//topic 
																						streamDefinition.getId() + "event", 	//key 
																						new Gson().toJson(event)); 	 			// message

			producer.send(message);
			
		}
		
	}
	
			
	
	private ProducerConfig createProducerConfig() {
		Properties properties = new Properties();
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		properties.put("metadata.broker.list", kafkaCluster);
//			properties.put("request.required.acks", "1");
//			properties.put("compress", "true");
//			properties.put("compression.codec", "gzip");
//			properties.put("producer.type", "sync");
 
        return new ProducerConfig(properties);
    }
	
	public void shutdownCallback() {
		this.producer.close();
	}

	@Override
	public void onMessage(Message<String> message) {
		if (message.getMessageObject().equalsIgnoreCase(streamDefinition.getStreamId())) {
			shutdownCallback();
			logger.debug("Shutting down listener for stream " + streamDefinition.getStreamId());
		}
		
	}
		
}