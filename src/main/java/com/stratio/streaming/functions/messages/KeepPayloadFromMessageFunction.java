package com.stratio.streaming.functions.messages;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import com.google.gson.Gson;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;

public class KeepPayloadFromMessageFunction extends Function<Tuple2<String, String>, StratioStreamingMessage> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7911766880059394316L;

	public KeepPayloadFromMessageFunction() {
		
	}

	@Override
	public StratioStreamingMessage call(Tuple2<String, String> message) {
		
		return buildRequestMessage(message._1(), message._2());
	}

	
	private StratioStreamingMessage buildRequestMessage(String operation, String requestJson) {
		
		StratioStreamingMessage requestMessage = null;
		
		requestMessage = getGson().fromJson(requestJson, StratioStreamingMessage.class);		
		requestMessage.setOperation(operation);
		
		
		return requestMessage;
		
	}

	
	
	private Gson getGson() {
		return new Gson();
	}


}
