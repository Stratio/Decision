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
package com.stratio.decision.drools;

import org.kie.api.builder.KieScanner;
import org.kie.api.runtime.KieContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.decision.drools.sessions.DroolsSession;
import com.stratio.decision.drools.sessions.DroolsStatefulSession;
import com.stratio.decision.drools.sessions.DroolsStatelessSession;

/**
 * Created by jmartinmenor on 22/12/15.
 */
public class DroolsInstace {


    private static final Logger logger = LoggerFactory.getLogger(DroolsInstace.class);

    KieContainer kieContainer;
    KieScanner kScanner;
    DroolsSession session;

    public DroolsInstace(KieContainer kieContainer, String sessionName, String sessionType){

        this.kieContainer = kieContainer;
        instanceSession(sessionName, sessionType);
    }

    private  void instanceSession(String sessionName, String sessionType){

        try {
            switch (sessionType) {
            case "stateful":
                setSession(new DroolsStatefulSession(kieContainer, sessionName));
                break;
            case "stateless":
                setSession(new DroolsStatelessSession(kieContainer, sessionName));
                break;
            }
        }
        catch (Exception e) {
            logger.error("Error creating Drools session {}: {}", sessionName, e.getMessage());
            logger.error("Please, check your Drools configuration if you want to use the send to Drools action.");
        }
    }

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
