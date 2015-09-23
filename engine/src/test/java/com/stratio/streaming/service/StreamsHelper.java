package com.stratio.streaming.service;

import com.stratio.streaming.commons.constants.ColumnType;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by aitor on 9/22/15.
 */
public abstract class StreamsHelper {

    // Helper test info #1
    public static final String STREAM_NAME= "testStream";

    public static final String RESULT_STREAM_NAME= "resultStream";

    public static final String STREAM_DEFINITION = "define stream " + STREAM_NAME + " (name string, timestamp double, " +
            "value int, enabled bool, numberl long, numberf float)";

    public static final String QUERY = " from "+ STREAM_NAME+ "[value >= 50] select name,timestamp,value " +
            "insert into " + RESULT_STREAM_NAME+ " ;";



    public static final String QUERY_ID = "testQuery";

    public static final List<ColumnNameTypeValue> COLUMNS= new LinkedList<ColumnNameTypeValue>() {{
        add(new ColumnNameTypeValue("name", ColumnType.STRING, "name"));
        add(new ColumnNameTypeValue("timestamp", ColumnType.DOUBLE, "timestamp"));
        add(new ColumnNameTypeValue("value", ColumnType.INTEGER, "value"));
        add(new ColumnNameTypeValue("enabled", ColumnType.BOOLEAN, "enabled"));
        add(new ColumnNameTypeValue("numberl", ColumnType.LONG, "long"));
        add(new ColumnNameTypeValue("numberf", ColumnType.FLOAT, "float"));
    }};

    // Helper test info #2
    public static final String STREAM_NAME2= "testStream2";

    public static final String RESULT_STREAM_NAME2= "resultStream2";

    public static final String QUERY2 = " from "+ STREAM_NAME2+ "[valuew < 10] select name2,timestamp2,value2 " +
            "insert into "+ RESULT_STREAM_NAME2 + " ;";

    public static final String QUERY_ID2 = "testQuery2";

    public static final List<ColumnNameTypeValue> COLUMNS2= new LinkedList<ColumnNameTypeValue>() {{
        add(new ColumnNameTypeValue("name2", ColumnType.STRING, "name2"));
        add(new ColumnNameTypeValue("timestamp2", ColumnType.DOUBLE, "timestamp2"));
        add(new ColumnNameTypeValue("value2", ColumnType.INTEGER, "value2"));
        add(new ColumnNameTypeValue("enabled2", ColumnType.BOOLEAN, "enabled2"));
    }};


    public static final String ACTION_LISTEN_TOKEN= "[LISTEN]";

}
