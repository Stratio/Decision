package com.stratio.streaming.commons.messages;

import java.util.List;

public class ListStreamsMessage {
	
	private Integer count;
	private Long timestamp;
	private List<StratioStreamingMessage> streams;

	public ListStreamsMessage() {
		// TODO Auto-generated constructor stub
	}
	
	

	/**
	 * @param count
	 * @param timestamp
	 * @param streams
	 */
	public ListStreamsMessage(Integer count, 
							   Long timestamp,
							   	List<StratioStreamingMessage> streams) {
		super();
		this.count = count;
		this.timestamp = timestamp;
		this.streams = streams;
	}



	public Integer getCount() {
		return count;
	}

	public void setCount(Integer count) {
		this.count = count;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public List<StratioStreamingMessage> getStreams() {
		return streams;
	}

	public void setStreams(List<StratioStreamingMessage> streams) {
		this.streams = streams;
	}
}
