package com.stratio.streaming.commons.constants;

public interface STREAMING {
	public static final int    DURATION_MS						= 2000;
	public static final String ZK_BASE_PATH						= "/stratio/streaming";
	public static final String STREAM_STATUS_MAP 				= "stratio_stream_map";
	public static final String INTERNAL_LISTEN_TOPIC 			= "stratio_listen";
	public static final String INTERNAL_SAVE2CASSANDRA_TOPIC 	= "stratio_save2cassandra";
	public static final String ZK_EPHEMERAL_NODE_PATH			= "/stratio/streaming/engine";
	public static final String STREAMING_KEYSPACE_NAME 			= "stratio_streaming";
	public static final String CREATE_STREAMING_KEYSPACE 		= "CREATE KEYSPACE " + STREAMING_KEYSPACE_NAME + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}";
	


	public interface STATS_NAMES {
		public static final String BASE 						= "stratio_stats_base";
		public static final String GLOBAL_STATS_BY_OPERATION 	= "stratio_stats_global_by_operation";
		public static final String[] RESERVED_STREAMS 			= new String[]{BASE, GLOBAL_STATS_BY_OPERATION};
		
	}

}
