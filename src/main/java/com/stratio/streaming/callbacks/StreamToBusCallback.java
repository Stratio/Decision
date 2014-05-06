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

public class StreamToBusCallback extends StreamCallback implements MessageListener<String> {

	private static Logger logger = LoggerFactory.getLogger(StreamToBusCallback.class);
	
	private StreamDefinition streamDefinition;
	private String kafkaCluster;
	private Producer<String, String> producer;
	private Boolean running;
	
	public StreamToBusCallback(StreamDefinition streamDefinition, String kafkaCluster) {
		this.streamDefinition = streamDefinition;
		this.kafkaCluster = kafkaCluster;
		this.producer = new Producer<String, String>(createProducerConfig());
		running = Boolean.TRUE;
		logger.debug("Starting listener for stream " + streamDefinition.getStreamId());
	}
	
	@Override
	public void receive(Event[] events) {
		
		if (running) {
			
		
			List<StratioStreamingMessage> collected_events = Lists.newArrayList();
			
			for (Event e : events) {
				
//				logger.info("event type: " + e.getClass() + " - " + e.getData(0) + e.getData(1));
				
				
				
				if (e instanceof InEvent) {
					InEvent ie = (InEvent) e;
					
					List<ColumnNameTypeValue> columns = Lists.newArrayList();
					
					for (Attribute column : streamDefinition.getAttributeList()) {
						
//						logger.info("event data size: " +ie.getData().length + " // attribute: " + column.getName() + " // position: " + streamDefinition.getAttributePosition(column.getName()));
						
//						avoid retrieving a value out of the scope
//						outputStream could have more fields defined than the output events (projection)
						if (ie.getData().length >= streamDefinition.getAttributePosition(column.getName()) + 1) {
						
							columns.add(new ColumnNameTypeValue(column.getName(),
								 								column.getType().toString(),
								 								ie.getData(streamDefinition.getAttributePosition(column.getName()))));
						}
						
						
					}
					
					
					collected_events.add(new StratioStreamingMessage(streamDefinition.getStreamId(), 	// value.streamName
																				ie.getTimeStamp(), 	//value.timestamp
																				columns)); 	 		// value.columns															
										
				}
			}
			
			sendEventsToBus(collected_events);
		}
		
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
		properties.put("producer.type", "async");
//			properties.put("request.required.acks", "1");
//			properties.put("compress", "true");
//			properties.put("compression.codec", "gzip");

 
        return new ProducerConfig(properties);
    }
	
	private void shutdownCallback() {
		if (running) {
			this.producer.close();
		}
	}

	@Override
	public void onMessage(Message<String> message) {
		if (running) {
			if (message.getMessageObject().equalsIgnoreCase(streamDefinition.getStreamId())
					|| message.getMessageObject().equalsIgnoreCase("*")) {
				
				shutdownCallback();
				running = Boolean.FALSE;
				logger.debug("Shutting down listener for stream " + streamDefinition.getStreamId());
			}
		}
		
	}
		
}