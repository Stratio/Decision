package com.stratio.decision.drools.sessions;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by josepablofernandez on 3/12/15.
 */
public class Results {

    private List cepResults;
    private List kafkaResults;

    public Results(){
        cepResults = new ArrayList();
        kafkaResults = new ArrayList();
    }

    public List getCepResults() {
        return cepResults;
    }

    public List getKafkaResults() {
        return kafkaResults;
    }

    public boolean addCepResults(Object o) {
        return cepResults.add(o);
    }

    public boolean addKafkaResults(Object o) {
        return kafkaResults.add(o);
    }
}
