package com.stratio.decision.functions.engine;

import java.util.ArrayList;
import java.util.List;

import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;

import com.stratio.decision.commons.constants.InternalTopic;
import com.stratio.decision.commons.messages.ColumnNameTypeValue;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.drools.DroolsConnectionContainer;
import com.stratio.decision.drools.configuration.DroolsConfigurationGroupBean;
import com.stratio.decision.drools.sessions.DroolsSession;
import com.stratio.decision.drools.sessions.DroolsStatefulSession;
import com.stratio.decision.drools.sessions.DroolsStatelessSession;
import com.stratio.decision.drools.sessions.Results;
import com.stratio.decision.serializer.Serializer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

/**
 * Created by josepablofernandez on 2/12/15.
 */
public class DroolsEngineAction extends BaseEngineAction {

    private static final Logger log = LoggerFactory.getLogger(DroolsEngineAction.class);

    private DroolsConnectionContainer droolsConnectionContainer;
    private List<String> groups;

    public DroolsEngineAction(DroolsConnectionContainer droolsConnectionContainer, Object[] actionParameters,
            Producer<String, String> producer,
            Serializer<String,
            StratioStreamingMessage> kafkaToJavaSerializer,
            Serializer<StratioStreamingMessage, Event> javaToSiddhiSerializer, SiddhiManager siddhiManager){

        super(actionParameters, producer, kafkaToJavaSerializer, javaToSiddhiSerializer, siddhiManager);

        this.droolsConnectionContainer = droolsConnectionContainer;
        this.groups = new ArrayList<>();

        for (Object actionParameter : actionParameters) {

            groups.add((String) actionParameter);
        }


    }


    public DroolsEngineAction(DroolsConnectionContainer droolsConnectionContainer, Object[] actionParameters,
            SiddhiManager siddhiManager) {

        super(actionParameters, siddhiManager);

        this.droolsConnectionContainer = droolsConnectionContainer;
        this.groups = new ArrayList<>();

        for (Object actionParameter : actionParameters) {

            groups.add((String) actionParameter);
        }


    }


    @Override
    public void execute(String streamName, Event[] inEvents) {

        if (log.isDebugEnabled()) {

            log.debug("Firing rules for stream {}", streamName);
        }

        KieContainer kContainer;
        DroolsSession session = null;

        for (String groupName : groups){

            kContainer = droolsConnectionContainer.getGroupContainer(groupName);

            if (kContainer == null) {

                if (log.isDebugEnabled()) {
                    log.debug("EXECUTED for groupName {}", groupName);
                }

                return;

            }

            DroolsConfigurationGroupBean groupConfiguration = droolsConnectionContainer.getGroupConfiguration
                    (groupName);

            String sessionType = groupConfiguration.getSessionType();
            String sessionName = groupConfiguration.getSessionName();

            switch (sessionType) {
            case "stateful" : session = new DroolsStatefulSession(kContainer, sessionName);
                              break;
            case "stateless" : session = new DroolsStatelessSession(kContainer, sessionName);
                               break;
            }


            if (session != null) {

                // TODO Inject Facts into the session
                // TODO  Mapping Event[] events to List

                /*
                for (Event event : inEvents) {

                    StratioStreamingMessage messageObject = javaToSiddhiSerializer.deserialize(event);

                    List<ColumnNameTypeValue> columnNameTypeValues = messageObject.getColumns();
                    for ( ColumnNameTypeValue column : columnNameTypeValues){
                        column.getColumn();
                        column.getType();
                    }
                }
                */


                List data = new ArrayList();
                data.add("comienzo test");

                Results results = session.fireRules(data);

            } else {

                if (log.isDebugEnabled()) {

                    log.debug("No Drools Session instance for stream {}", streamName);
                }

            }

            // TODO Handler for output facts
            // Collection<FactHandle> factHandles = session.getFactHandles();

        }



        if (log.isDebugEnabled()) {

            log.debug("Finished rules processing for stream {}", streamName);
        }



    }
}
