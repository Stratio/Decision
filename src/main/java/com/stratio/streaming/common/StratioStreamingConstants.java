package com.stratio.streaming.common;

public interface StratioStreamingConstants {


	public interface BUS {
		public static final String STREAMING_GROUP_ID 	= "stratioStreaming";
		public static final String TOPICS				= "stratio_streaming_requests";
		public static final String LIST_STREAMS_TOPIC	= "stratio_streaming_list_streams";
	}
	
	public interface STREAMING {
		public static final int    DURATION_MS		= 2000;
	}

	
	
	public interface REPLY_CODES {
		
		public static final String  ZK_BASE_PATH				= "/stratio/streaming";
		public static final Integer OK 							= 1;
		public static final Integer KO_PARSER_ERROR				= 2;
		public static final Integer KO_STREAM_ALREADY_EXISTS 	= 3;
		public static final Integer KO_STREAM_DOES_NOT_EXIST 	= 4;
		public static final Integer KO_QUERY_ALREADY_EXISTS 	= 5;
		public static final Integer KO_LISTENER_ALREADY_EXISTS 	= 6;		
		public static final Integer KO_GENERAL_ERROR			= 7;
		public static final Integer KO_COLUMN_ALREADY_EXISTS 	= 8;
		public static final Integer KO_COLUMN_DOES_NOT_EXIST 	= 9;
		
		
	}
	
	public interface CEP_OPERATIONS {
		public static final String CREATE_OPERATION 	= "CREATE";
		public static final String ADD_QUERY_OPERATION 	= "ADDQUERY";
		public static final String LISTEN_OPERATION 	= "LISTEN";
		public static final String DROP_OPERATION 		= "DROP";
		public static final String INSERT_OPERATION 	= "INSERT";
		public static final String ALTER_OPERATION 		= "ALTER";
		public static final String LIST_OPERATION 		= "LIST";
	}
	
	
	

}
