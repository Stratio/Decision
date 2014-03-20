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

import scala.Tuple2;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.stratio.streaming.functions.FilterMessagesByOperationFunction;
import com.stratio.streaming.messages.BaseStreamingMessage;
import com.stratio.streaming.messages.ColumnNameTypeValue;
import com.stratio.streaming.utils.DataToCollector;

public class StreamToBusCallback extends StreamCallback {

	private static Logger logger = LoggerFactory.getLogger(FilterMessagesByOperationFunction.class);
	
	private StreamDefinition streamDefinition;
	private String kafkaCluster;
	private Producer<String, String> producer;
	
	public StreamToBusCallback(StreamDefinition streamDefinition, String kafkaCluster) {
		this.streamDefinition = streamDefinition;
		this.kafkaCluster = kafkaCluster;
		this.producer = new Producer<String, String>(createProducerConfig());
	}
	
	@Override
	public void receive(Event[] events) {
		
		List<BaseStreamingMessage> collected_events = Lists.newArrayList();
		
		for (Event e : events) {
			if (e instanceof InEvent) {
				InEvent ie = (InEvent) e;
				
				List<ColumnNameTypeValue> columns = Lists.newArrayList();
				
				for (Attribute column : streamDefinition.getAttributeList()) {
					
					columns.add(new ColumnNameTypeValue(column.getName(),
							 								column.getType().toString(),
							 								ie.getData(streamDefinition.getAttributePosition(column.getName()))));
					
					
				}
				
				
				collected_events.add(new BaseStreamingMessage(streamDefinition.getStreamId(), 			// value.streamName
																			ie.getTimeStamp(), 	//value.timestamp
																			columns)); 	 		// value.columns															
									
			}
		}
		
		sendEventToBus(collected_events);
		sendEventToDataCollector(collected_events);
		
	}
	
	


	private void sendEventToDataCollector(List<BaseStreamingMessage> collected_events) {
		
		ColumnNameTypeValue indexColumn = new ColumnNameTypeValue("index", null, null);
		ColumnNameTypeValue dataColumn = new ColumnNameTypeValue("data", null, null);
		
		List<Tuple2<String, Object>> data = Lists.newArrayList();
		
		try {
		
			for (BaseStreamingMessage event : collected_events) {
				
				int indexPosition = event.getColumns().indexOf(indexColumn);
				int dataPosition  = event.getColumns().indexOf(dataColumn);
				
				if (indexPosition > 0 & dataPosition > 0) {
															
					data.add(new Tuple2<String, Object>(event.getColumns().get(indexPosition).getValue().toString().replace(".0", ""), 
																event.getColumns().get(dataPosition).getValue()));
					
					logger.info("sendEventToDataCollector:" + event.getColumns().get(indexPosition).getValue().toString());
				}
				
				
				
				
			}
			
		
			DataToCollector.sendDataToOpenSense(data);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	private void sendEventToBus(List<BaseStreamingMessage> collected_events) {
		
		for (BaseStreamingMessage event : collected_events) {
			
			KeyedMessage<String, String> message = new KeyedMessage<String, String>(streamDefinition.getId(), 				//topic 
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
		
	}