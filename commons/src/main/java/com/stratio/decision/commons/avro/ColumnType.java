/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.decision.commons.avro;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class ColumnType extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 6385653382887656308L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"ColumnType\",\"namespace\":\"com.stratio.decision.commons.avro\",\"fields\":[{\"name\":\"column\",\"type\":\"string\"},{\"name\":\"value\",\"type\":\"string\"},{\"name\":\"type\",\"type\":\"string\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.CharSequence column;
  @Deprecated public java.lang.CharSequence value;
  @Deprecated public java.lang.CharSequence type;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>. 
   */
  public ColumnType() {}

  /**
   * All-args constructor.
   */
  public ColumnType(java.lang.CharSequence column, java.lang.CharSequence value, java.lang.CharSequence type) {
    this.column = column;
    this.value = value;
    this.type = type;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return column;
    case 1: return value;
    case 2: return type;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: column = (java.lang.CharSequence)value$; break;
    case 1: value = (java.lang.CharSequence)value$; break;
    case 2: type = (java.lang.CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'column' field.
   */
  public java.lang.CharSequence getColumn() {
    return column;
  }

  /**
   * Sets the value of the 'column' field.
   * @param value the value to set.
   */
  public void setColumn(java.lang.CharSequence value) {
    this.column = value;
  }

  /**
   * Gets the value of the 'value' field.
   */
  public java.lang.CharSequence getValue() {
    return value;
  }

  /**
   * Sets the value of the 'value' field.
   * @param value the value to set.
   */
  public void setValue(java.lang.CharSequence value) {
    this.value = value;
  }

  /**
   * Gets the value of the 'type' field.
   */
  public java.lang.CharSequence getType() {
    return type;
  }

  /**
   * Sets the value of the 'type' field.
   * @param value the value to set.
   */
  public void setType(java.lang.CharSequence value) {
    this.type = value;
  }

  /**
   * Creates a new ColumnType RecordBuilder.
   * @return A new ColumnType RecordBuilder
   */
  public static com.stratio.decision.commons.avro.ColumnType.Builder newBuilder() {
    return new com.stratio.decision.commons.avro.ColumnType.Builder();
  }
  
  /**
   * Creates a new ColumnType RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new ColumnType RecordBuilder
   */
  public static com.stratio.decision.commons.avro.ColumnType.Builder newBuilder(com.stratio.decision.commons.avro.ColumnType.Builder other) {
    return new com.stratio.decision.commons.avro.ColumnType.Builder(other);
  }
  
  /**
   * Creates a new ColumnType RecordBuilder by copying an existing ColumnType instance.
   * @param other The existing instance to copy.
   * @return A new ColumnType RecordBuilder
   */
  public static com.stratio.decision.commons.avro.ColumnType.Builder newBuilder(com.stratio.decision.commons.avro.ColumnType other) {
    return new com.stratio.decision.commons.avro.ColumnType.Builder(other);
  }
  
  /**
   * RecordBuilder for ColumnType instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<ColumnType>
    implements org.apache.avro.data.RecordBuilder<ColumnType> {

    private java.lang.CharSequence column;
    private java.lang.CharSequence value;
    private java.lang.CharSequence type;

    /** Creates a new Builder */
    private Builder() {
      super(com.stratio.decision.commons.avro.ColumnType.SCHEMA$);
    }
    
    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(com.stratio.decision.commons.avro.ColumnType.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.column)) {
        this.column = data().deepCopy(fields()[0].schema(), other.column);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.value)) {
        this.value = data().deepCopy(fields()[1].schema(), other.value);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.type)) {
        this.type = data().deepCopy(fields()[2].schema(), other.type);
        fieldSetFlags()[2] = true;
      }
    }
    
    /**
     * Creates a Builder by copying an existing ColumnType instance
     * @param other The existing instance to copy.
     */
    private Builder(com.stratio.decision.commons.avro.ColumnType other) {
            super(com.stratio.decision.commons.avro.ColumnType.SCHEMA$);
      if (isValidValue(fields()[0], other.column)) {
        this.column = data().deepCopy(fields()[0].schema(), other.column);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.value)) {
        this.value = data().deepCopy(fields()[1].schema(), other.value);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.type)) {
        this.type = data().deepCopy(fields()[2].schema(), other.type);
        fieldSetFlags()[2] = true;
      }
    }

    /**
      * Gets the value of the 'column' field.
      * @return The value.
      */
    public java.lang.CharSequence getColumn() {
      return column;
    }

    /**
      * Sets the value of the 'column' field.
      * @param value The value of 'column'.
      * @return This builder.
      */
    public com.stratio.decision.commons.avro.ColumnType.Builder setColumn(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.column = value;
      fieldSetFlags()[0] = true;
      return this; 
    }

    /**
      * Checks whether the 'column' field has been set.
      * @return True if the 'column' field has been set, false otherwise.
      */
    public boolean hasColumn() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'column' field.
      * @return This builder.
      */
    public com.stratio.decision.commons.avro.ColumnType.Builder clearColumn() {
      column = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'value' field.
      * @return The value.
      */
    public java.lang.CharSequence getValue() {
      return value;
    }

    /**
      * Sets the value of the 'value' field.
      * @param value The value of 'value'.
      * @return This builder.
      */
    public com.stratio.decision.commons.avro.ColumnType.Builder setValue(java.lang.CharSequence value) {
      validate(fields()[1], value);
      this.value = value;
      fieldSetFlags()[1] = true;
      return this; 
    }

    /**
      * Checks whether the 'value' field has been set.
      * @return True if the 'value' field has been set, false otherwise.
      */
    public boolean hasValue() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'value' field.
      * @return This builder.
      */
    public com.stratio.decision.commons.avro.ColumnType.Builder clearValue() {
      value = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'type' field.
      * @return The value.
      */
    public java.lang.CharSequence getType() {
      return type;
    }

    /**
      * Sets the value of the 'type' field.
      * @param value The value of 'type'.
      * @return This builder.
      */
    public com.stratio.decision.commons.avro.ColumnType.Builder setType(java.lang.CharSequence value) {
      validate(fields()[2], value);
      this.type = value;
      fieldSetFlags()[2] = true;
      return this; 
    }

    /**
      * Checks whether the 'type' field has been set.
      * @return True if the 'type' field has been set, false otherwise.
      */
    public boolean hasType() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'type' field.
      * @return This builder.
      */
    public com.stratio.decision.commons.avro.ColumnType.Builder clearType() {
      type = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    @Override
    public ColumnType build() {
      try {
        ColumnType record = new ColumnType();
        record.column = fieldSetFlags()[0] ? this.column : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.value = fieldSetFlags()[1] ? this.value : (java.lang.CharSequence) defaultValue(fields()[1]);
        record.type = fieldSetFlags()[2] ? this.type : (java.lang.CharSequence) defaultValue(fields()[2]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  private static final org.apache.avro.io.DatumWriter
    WRITER$ = new org.apache.avro.specific.SpecificDatumWriter(SCHEMA$);  

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, org.apache.avro.specific.SpecificData.getEncoder(out));
  }

  private static final org.apache.avro.io.DatumReader
    READER$ = new org.apache.avro.specific.SpecificDatumReader(SCHEMA$);  

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, org.apache.avro.specific.SpecificData.getDecoder(in));
  }

}
