package com.stratio.streaming.messages;

import java.io.Serializable;

public class ColumnNameTypeValue implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 4729943886618928365L;
	private String column;
	private String type;
	private Object value;

	public ColumnNameTypeValue() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param column
	 * @param type
	 * @param value
	 */
	public ColumnNameTypeValue(String column, String type, Object value) {
		super();
		this.column = column;
		this.type = type;
		this.value = value;
	}



	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}



	public Object getValue() {
		return value;
	}



	public void setValue(Object value) {
		this.value = value;
	}

	

	
	
}
