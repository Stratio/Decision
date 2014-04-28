package com.stratio.streaming.streams;

import java.io.Serializable;
import java.util.HashMap;


public class StreamStatusDTO implements Serializable {
	
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -8455557383950387810L;
	private String streamName;
	private String streamDefinition;
	private Boolean userDefined;
	private Boolean listen_enabled;
	private Boolean saveToCassandra_enabled;
	private HashMap<String, String> addedQueries;
	
	
	/**
	 * @param streamName
	 * @param listen_enabled
	 * @param saveToCassandra_enabled
	 */
	public StreamStatusDTO(String streamName, Boolean userDefined) {
		super();
		this.streamName = streamName;
		this.userDefined = userDefined;
		this.listen_enabled = false;
		this.saveToCassandra_enabled = false;
		this.addedQueries = new HashMap<String, String>();
	}
	
	
	public Boolean isUserDefined() {
		return userDefined;
	}


	public void setUserDefined(Boolean userDefined) {
		this.userDefined = userDefined;
	}



	public String getStreamName() {
		return streamName;
	}
	public void setStreamName(String streamName) {
		this.streamName = streamName;
	}
	public Boolean isListen_enabled() {
		return listen_enabled;
	}
		
	public void setListen_enabled(Boolean listen_enabled) {
		this.listen_enabled = listen_enabled;
	}
	public Boolean isSaveToCassandra_enabled() {
		return saveToCassandra_enabled;
	}
	public void setSaveToCassandra_enabled(Boolean saveToCassandra_enabled) {
		this.saveToCassandra_enabled = saveToCassandra_enabled;
	}

	

	public String getStreamDefinition() {
		return streamDefinition;
	}


	public void setStreamDefinition(String streamDefinition) {
		this.streamDefinition = streamDefinition;
	}


	public HashMap<String, String> getAddedQueries() {
		return addedQueries;
	}


	public void setAddedQueries(HashMap<String, String> addedQueries) {
		this.addedQueries = addedQueries;
	}

	
	
}
