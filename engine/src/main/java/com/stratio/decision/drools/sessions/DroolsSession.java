package com.stratio.decision.drools.sessions;

import java.util.List;

/**
 * Created by josepablofernandez on 3/12/15.
 */
public interface DroolsSession {

    public static final String QUERY_CEP_NAME= "cepresult";
    public static final String QUERY_KAFKA_NAME= "kafkaresult";
    public static final String QUERY_CEP_RESULT = "cres";
    public static final String QUERY_KAFKA_RESULT = "kres";

    public Results fireRules(List data);

}
