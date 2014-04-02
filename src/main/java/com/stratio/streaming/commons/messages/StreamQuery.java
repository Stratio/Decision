package com.stratio.streaming.commons.messages;

import java.io.Serializable;

public class StreamQuery implements Serializable {

	private static final long serialVersionUID = 4790891263734101673L;

	private String queryId;
	private String query;
	
	public StreamQuery(String queryId, String query) {
		this.queryId = queryId;
		this.query = query;
	}

	public String getQueryId() {
		return queryId;
	}

	public void setQueryId(String queryId) {
		this.queryId = queryId;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}



}
