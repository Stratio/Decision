package com.stratio.streaming.callbacks;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

public class StreamToDataCollectorCallback extends StreamCallback {


		private StreamDefinition streamDefinition;
		
		public StreamToDataCollectorCallback(StreamDefinition streamDefinition, String kafkaCluster) {
			this.streamDefinition = streamDefinition;
		}
		
		@Override
		public void receive(Event[] events) {
			
			for (Event e : events) {
				if (e instanceof InEvent) {
					InEvent ie = (InEvent) e;
					
//						TODO
				}
			}
			
		}		
	}