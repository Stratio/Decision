package com.stratio.decision.drools;

import java.util.HashMap;
import java.util.Map;

import org.apache.maven.settings.Mirror;
import org.apache.maven.settings.Server;
import org.kie.api.KieServices;
//import org.kie.api.builder.KieScanner;
import org.kie.api.runtime.KieContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.decision.drools.configuration.DroolsConfigurationBean;
import com.stratio.decision.drools.configuration.DroolsConfigurationGroupBean;
import com.stratio.decision.drools.sessions.DroolsStatefulSession;
import com.stratio.decision.drools.sessions.DroolsStatelessSession;

import scala.tools.cmd.gen.AnyVals;

/**
 * Created by josepablofernandez on 2/12/15.
 */
public class DroolsConnectionContainer {


    private static final Logger logger = LoggerFactory.getLogger(DroolsConnectionContainer.class);

    private Map<String, DroolsInstace> groupContainers;
    private Map<String, DroolsConfigurationGroupBean> groupConfigurations;


    public DroolsConnectionContainer(DroolsConfigurationBean droolsConfigurationBean) {

        this.groupContainers = configGroupsContainers(droolsConfigurationBean);
        this.groupConfigurations = droolsConfigurationBean.getGroups();
    }

    private Map<String, String> configGroupsSessionType(DroolsConfigurationBean droolsConfigurationBean) {

        Map<String, String> groupSessionTypes = new HashMap<>();

        if (droolsConfigurationBean != null){

            for (String groupName : droolsConfigurationBean.getListGroups()){

                DroolsConfigurationGroupBean groupConfigBean = droolsConfigurationBean.getGroups().get(groupName);
                groupSessionTypes.put(groupName, groupConfigBean.getSessionType());

            }
        }

        return groupSessionTypes;
    }

    private Map<String, DroolsInstace> configGroupsContainers(DroolsConfigurationBean droolsConfigurationBean){

        Map<String, DroolsInstace> groupContainers = new HashMap<>();

        if (droolsConfigurationBean != null) {

            if (droolsConfigurationBean.getListGroups() == null){

                logger.warn("Drools Configuration is not found. Please, check your configuration if you want to use "
                        + "the send to drools action");

                return groupContainers;
            }

            KieServices ks = KieServices.Factory.get();

            for (String groupName : droolsConfigurationBean.getListGroups()){


                DroolsConfigurationGroupBean groupConfigBean = droolsConfigurationBean.getGroups().get(groupName);

                KieContainer groupContainer = null;

                try {
                    groupContainer = ks
                            .newKieContainer(
                                    ks.newReleaseId(groupConfigBean.getGroupId(), groupConfigBean.getArtifactId(),
                                            groupConfigBean.getVersion())
                            );
                }catch (Exception e){
                   logger.error("Error creating Drools KieContainer: " + e.getMessage());
                   logger.error("Please, check your Drools Configuration if you want to use the send to drools action"
                           + ".");
                }

                if (groupContainer != null) {

                    DroolsInstace instance = new DroolsInstace(groupContainer, groupConfigBean.getSessionName(),
                            groupConfigBean.getSessionType());

                    // TODO Add Scanner to the container if it is required
                    instance.setkScanner(ks.newKieScanner(groupContainer));
                    instance.getkScanner().start(groupConfigBean.getScanFrequency());

                    groupContainers.put(groupName, instance);

                } else {

                    if (logger.isDebugEnabled()){
                        logger.debug("It was not possible to create a KieContainer for group {}", groupName);
                    }
                }

            }

        }

        return groupContainers;
    }

    public Map<String, DroolsInstace> getGroupContainers() {

        return groupContainers;
    }

    public DroolsInstace getGroupContainer(String groupName) {

        return groupContainers.get(groupName);
    }

    public Map<String, DroolsConfigurationGroupBean> getGroupConfigurations(){

        return groupConfigurations;
    }

    public DroolsConfigurationGroupBean getGroupConfiguration(String groupName) {

        return groupConfigurations.get(groupName);
    }

}
