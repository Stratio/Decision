package com.stratio.decision.drools.sessions;

import java.util.Iterator;
import java.util.List;

import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.QueryResultsRow;

/**
 * Created by josepablofernandez on 3/12/15.
 */
public class DroolsStatefulSession implements DroolsSession {

    private final KieContainer kContainer;
    private final KieSession session;

    public DroolsStatefulSession(KieContainer kContainer, String sessionName){
        this.kContainer = kContainer;
        this.session = this.kContainer.newKieSession(sessionName);
        //this.kServices = KieServices.Factory.get();
    }


    public Results fireRules(List data) {

        Results res = new Results();
        for(Object i : data) {
            session.insert(i);
        }

        int n = session.fireAllRules();

        this.addResults(res.getCepResults(), QUERY_CEP_NAME, QUERY_CEP_RESULT);
        this.addResults(res.getKafkaResults(), QUERY_KAFKA_NAME, QUERY_KAFKA_RESULT);

        return res;
    }

    private void addResults(List r, String nameQuery, String nameResult){

        Iterator<QueryResultsRow> rows = session.getQueryResults(nameQuery).iterator();
        while (rows.hasNext()) {
            QueryResultsRow row = rows.next();
            r.add( row.get(nameResult));
        }


    }
}
