package com.stratio.decision.drools.sessions;

import java.util.List;

/**
 * Created by josepablofernandez on 3/12/15.
 */
public interface DroolsSession {

    public static final String QUERY_NAME= "result";
    public static final String QUERY_RESULT = "res";

    public Results fireRules(List data);

}
