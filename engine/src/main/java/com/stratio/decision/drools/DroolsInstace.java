package com.stratio.decision.drools;

import org.kie.api.builder.KieScanner;
import org.kie.api.runtime.KieContainer;

import com.stratio.decision.drools.sessions.DroolsSession;

/**
 * Created by jmartinmenor on 22/12/15.
 */
public class DroolsInstace {

    KieContainer kieContainer;
    KieScanner kScanner;
    DroolsSession session;

    public KieContainer getKieContainer() {
        return kieContainer;
    }

    public void setKieContainer(KieContainer kieContainer) {
        this.kieContainer = kieContainer;
    }

    public KieScanner getkScanner() {
        return kScanner;
    }

    public void setkScanner(KieScanner kScanner) {
        this.kScanner = kScanner;
    }

    public DroolsSession getSession() {
        return session;
    }

    public void setSession(DroolsSession session) {
        this.session = session;
    }
}
