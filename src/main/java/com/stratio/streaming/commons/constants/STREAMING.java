package com.stratio.streaming.commons.constants;

public interface STREAMING {
	public static final int    DURATION_MS						= 2000;
	public static final String ZK_BASE_PATH						= "/stratio/streaming";
	public static final String STREAM_STATUS_MAP 				= "stratio_stream_map";
	public static final String INTERNAL_LISTEN_TOPIC 			= "stratio_listen";
	public static final String INTERNAL_SAVE2CASSANDRA_TOPIC 	= "stratio_save2cassandra";
	public static final String ZK_EPHEMERAL_NODE_PATH			= "/stratio/streaming/engine";



}
