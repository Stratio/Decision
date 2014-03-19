package com.stratio.streaming.callbacks;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.stratio.streaming.messages.BaseStreamingMessage;
import com.stratio.streaming.messages.ColumnNameTypeValue;

public class SiddhiToKafkaStreamCallback extends StreamCallback {


		private StreamDefinition streamDefinition;
		private String kafkaCluster;
		private Producer<String, String> producer;
		
		public SiddhiToKafkaStreamCallback(StreamDefinition streamDefinition, String kafkaCluster) {
			this.streamDefinition = streamDefinition;
			this.kafkaCluster = kafkaCluster;
			this.producer = new Producer<String, String>(createProducerConfig());
		}
		
		@Override
		public void receive(Event[] events) {
			
			for (Event e : events) {
				if (e instanceof InEvent) {
					InEvent ie = (InEvent) e;
					
					List<ColumnNameTypeValue> columns = Lists.newArrayList();
					
					for (Attribute column : streamDefinition.getAttributeList()) {
						
						columns.add(new ColumnNameTypeValue(column.getName(),
								 								column.getType().toString(),
								 								ie.getData(streamDefinition.getAttributePosition(column.getName()))));
						
						
					}				
					
					
					KeyedMessage<String, String> message = new KeyedMessage<String, String>(streamDefinition.getId(), 				//topic 
																								streamDefinition.getId() + "event", //key 
																								new Gson().toJson( //value
																										new BaseStreamingMessage(streamDefinition.getStreamId(), // value.streamName
																																				ie.getTimeStamp(), //value.timestamp
																																				columns))); 	 // value.columns
					producer.send(message);
				}
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
		
	}