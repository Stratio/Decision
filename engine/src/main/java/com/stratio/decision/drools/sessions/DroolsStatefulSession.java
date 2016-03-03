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
    }


    public Results fireRules(List data) {

        Results res = new Results();
        for(Object i : data) {
            session.insert(i);
        }

        int n = session.fireAllRules();

        this.addResults(res.getResults(),  QUERY_NAME, QUERY_RESULT);

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
