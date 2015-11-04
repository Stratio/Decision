package com.stratio.decision.dto.drools.configuration;

/**
 * Created by jmartinmenor on 8/10/15.
 */
public class ConfigurationConstants {

    public static String VARIABLE = "${key}";
    public static String BASE = "group";

    // GLOBAL CONSTANTS
    public static String URL_BASE = "workbench.url.base";
    public static String USER_NAME = "workbench.user.name";
    public static String USER_PASS = "workbench.user.pass";

    // GROUPS CONSTANTS
    public static String SESSION_NAME = BASE + "." + VARIABLE ;
    public static String URL_WORKBECH = BASE + "." + VARIABLE + ".jar.workbench";
    public static String QUERY_NAME = BASE + "." + VARIABLE + ".query.name";
    public static String QUERY_RESULT_NAME = BASE + "." + VARIABLE + ".query.result.name";
    public static String INPUT_MODEL_LIST = BASE + "." + VARIABLE + ".input.model.list";
    public static String RESULT_TYPE= BASE + "." + VARIABLE + ".result.type";

}
