package com.stratio.streaming.commons.constants;

public interface STREAM_OPERATIONS {
//	DDL
	public interface DEFINITION {
		public static final String CREATE 					= "CREATE";
		public static final String ADD_QUERY 				= "ADD_QUERY";
		public static final String REMOVE_QUERY				= "REMOVE_QUERY";
		public static final String DROP 					= "DROP";
		public static final String ALTER 					= "ALTER";
	}
	
//	DML
	public interface MANIPULATION {
		public static final String INSERT 					= "INSERT";
		public static final String LIST 					= "LIST";
	}
	
//	DAL
	public interface ACTION {
		public static final String LISTEN 					= "LISTEN";
		public static final String STOP_LISTEN				= "STOP_LISTEN";
		public static final String SAVETO_DATACOLLECTOR 	= "SAVETO_DATACOLLECTOR";
		public static final String SAVETO_CASSANDRA		 	= "SAVETO_CASSANDRA";
	}
	
	

}
