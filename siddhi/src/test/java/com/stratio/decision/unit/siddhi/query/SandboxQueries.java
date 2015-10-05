package com.stratio.decision.unit.siddhi.query;

/**
 * Created by aitor on 9/21/15.
 */
public abstract class SandboxQueries {

    public static final String STREAM_SENSORS= "sensor_grid";

    public static final String STREAM_SENSORS_GRID_AVG= "sensor_grid_avg";

    public static final String STREAM_SENSORS_GRID_ALARMS= "sensor_grid_alarms";

    public static final String QUERY_CREATE_STREAM_SENSORS=  "define stream " + STREAM_SENSORS + " (" +
            "name string, data double); ";


    /**
     * Sandbox Queries
     */
    public static final String QUERY_250_AVG = "from " + STREAM_SENSORS + "#window.length(250) " +
            " select name, avg(data) as data group by name " +
            "insert into " + STREAM_SENSORS_GRID_AVG + " for current-events";

    public static final String QUERY_AVG_CPU_HIGHER_80 = "from " + STREAM_SENSORS_GRID_AVG + "[name=='cpu' and data > 80]" +
            "#window.timeBatch(2 seconds) select name, avg(data) as data, " +
            "'Alarm_intensive_CPU_load' as text  " +
            "insert into " + STREAM_SENSORS_GRID_ALARMS + " for current-events";

    public static final String QUERY_AVG_MEMORY_HIGHER_75 = "from " + STREAM_SENSORS_GRID_AVG + "[name=='memory' and data > 75]" +
            "#window.timeBatch(2 seconds) select name, avg(data) as data, " +
            "'Alarm_intensive_MEMORY_load' as text  " +
            "insert into " + STREAM_SENSORS_GRID_ALARMS + " for current-events";

    public static final String QUERY_AVG_CPU_OR_MEMORY = "from " + STREAM_SENSORS_GRID_AVG +
            "[(name=='cpu' and data > 90) or (name=='memory' and data > 80)]" +
            "#window.timeBatch(2 seconds) select name, avg(data) as data, " +
            "'Alarm_inminent_shutdown' as text  " +
            "insert into " + STREAM_SENSORS_GRID_ALARMS + " for current-events";


}
