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

	
	


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((column == null) ? 0 : column.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ColumnNameTypeValue other = (ColumnNameTypeValue) obj;
		if (column == null) {
			if (other.column != null)
				return false;
		} else if (!column.equals(other.column))
			return false;
		return true;
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
