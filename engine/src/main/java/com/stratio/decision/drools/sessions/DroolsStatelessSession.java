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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.kie.api.KieServices;
import org.kie.api.command.Command;
import org.kie.api.command.KieCommands;
import org.kie.api.runtime.ExecutionResults;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.StatelessKieSession;
import org.kie.api.runtime.rule.QueryResults;
import org.kie.api.runtime.rule.QueryResultsRow;
import org.kie.internal.command.CommandFactory;

/**
 * Created by josepablofernandez on 3/12/15.
 */
public class DroolsStatelessSession implements DroolsSession {

    private final KieContainer kContainer;
    private final StatelessKieSession session;
    private final KieServices kServices;

    public DroolsStatelessSession(KieContainer kContainer, String sessionName){
        this.kContainer = kContainer;
        this.session = this.kContainer.newStatelessKieSession(sessionName);
        this.kServices = KieServices.Factory.get();
    }

    public Results fireRules(List data) {
        Results res = new Results();

        KieCommands commands = kServices.getCommands();

        List<Command> cmds = new ArrayList<Command>();

        cmds.add(commands.newInsertElements(data));
        cmds.add(CommandFactory.newFireAllRules());
        cmds.add(CommandFactory.newQuery(QUERY_RESULT, QUERY_NAME));

        ExecutionResults er = session.execute(commands.newBatchExecution(cmds));

        this.addResults(res.getResults(), er, QUERY_RESULT);

        return res;
    }

    private void addResults(List r, ExecutionResults er, String name){

        Iterator<QueryResultsRow> rows = ((QueryResults) er.getValue(name)).iterator();
        while (rows.hasNext()) {
            QueryResultsRow row = rows.next();
            r.add( row.get(name));
        }


    }
}
