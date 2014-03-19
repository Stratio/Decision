package com.stratio.streaming.functions;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import com.google.gson.Gson;
import com.stratio.streaming.messages.BaseStreamingMessage;

public class KeepPayloadFromMessageFunction extends Function<Tuple2<String, String>, BaseStreamingMessage> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7911766880059394316L;

	public KeepPayloadFromMessageFunction() {
		
	}

	@Override
	public BaseStreamingMessage call(Tuple2<String, String> message) {
		
		return buildRequestMessage(message._1(), message._2());
	}

	
	private BaseStreamingMessage buildRequestMessage(String operation, String requestJson) {
		
		BaseStreamingMessage requestMessage = null;
		
		requestMessage = getGson().fromJson(requestJson, BaseStreamingMessage.class);		
		requestMessage.setOperation(operation);
		
		
		return requestMessage;
		
	}

	
	
	private Gson getGson() {
		return new Gson();
	}


}
