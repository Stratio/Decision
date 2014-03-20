package com.stratio.streaming.functions;

import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class FilterMessagesByOperationFunction extends Function<Tuple2<String, String>, Boolean> {
	
	private static Logger logger = LoggerFactory.getLogger(FilterMessagesByOperationFunction.class);
	private String allowedOperation;

	/**
	 * 
	 */
	private static final long serialVersionUID = 7911766880059394316L;

	public FilterMessagesByOperationFunction(String operation) {
		this.allowedOperation = operation;
	}

	@Override
	public Boolean call(Tuple2<String, String> message) throws Exception {
		
		if (message._1() != null && message._1().equalsIgnoreCase(allowedOperation)) {
			return true;
		}
		return false;
	}

	
	



}
