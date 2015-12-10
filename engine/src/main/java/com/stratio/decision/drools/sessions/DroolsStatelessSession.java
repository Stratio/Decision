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
        cmds.add(commands.newQuery(QUERY_KAFKA_RESULT,QUERY_KAFKA_NAME));
        cmds.add(CommandFactory.newQuery(QUERY_CEP_RESULT,QUERY_CEP_NAME));

        ExecutionResults er = session.execute(commands.newBatchExecution(cmds));

        this.addResults(res.getKafkaResults(),er, QUERY_KAFKA_RESULT);
        this.addResults(res.getCepResults(),er, QUERY_CEP_RESULT);

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
