package com.stratio.decision.dto.drools.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.runtime.rule.QueryResults;
import org.kie.api.runtime.rule.QueryResultsRow;

import com.stratio.decision.dto.drools.configuration.model.DroolsConfigurationBean;
import com.stratio.decision.dto.drools.configuration.model.DroolsConfigurationGroupBean;

/**
 * Created by jmartinmenor on 13/10/15.
 */
public class DroolsClientImpl<T> implements DroolsClient<T> {

    private static String AUTH_ENABLED = "enabled";

    private KieSession session;
    private DroolsConfigurationBean conf;
    private DroolsConfigurationGroupBean group;

    public DroolsClientImpl(String groupName, DroolsConfigurationBean dc) throws IOException {

        this.conf = dc;
        this.group = dc.getGroup(groupName);
        loadLocalSession();
    }

    private void loadLocalSession() {

        KieServices ks = KieServices.Factory.get();
        KieContainer kContainer = ks.getKieClasspathContainer();

        this.session = kContainer.newKieSession(this.group.getSessionName());
    }



    public List fireRules(List<Object> dataList) {

        List res = new ArrayList<T>();

        for(Object data : dataList){
            session.insert(data);
        }
        session.fireAllRules();

        QueryResults queryResults = session.getQueryResults(this.group.getQueryName());
        Iterator<QueryResultsRow> rows = queryResults.iterator();
        while (rows.hasNext()) {
            QueryResultsRow row = rows.next();
            res.add( row.get(this.group.getResultName()));
        }

        Collection<FactHandle> facts = session.getFactHandles();

        FactHandle[] factArray = facts.toArray(new FactHandle[facts.size()]);
        for (int cont=0;cont<factArray.length;cont++) {
            session.delete(factArray[cont]);
        }

        return res;
    }


}
