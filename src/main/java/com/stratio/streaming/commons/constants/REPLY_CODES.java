package com.stratio.streaming.commons.constants;

public interface REPLY_CODES {
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
