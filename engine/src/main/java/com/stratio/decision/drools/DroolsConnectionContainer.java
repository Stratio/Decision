package com.stratio.decision.drools;

import java.util.HashMap;
import java.util.Map;

import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.decision.drools.configuration.DroolsConfigurationBean;
import com.stratio.decision.drools.configuration.DroolsConfigurationGroupBean;

import scala.tools.cmd.gen.AnyVals;

/**
 * Created by josepablofernandez on 2/12/15.
 */
public class DroolsConnectionContainer {


    private static final Logger log = LoggerFactory.getLogger(DroolsConnectionContainer.class);

    private Map<String, KieContainer> groupContainers;
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

    private Map<String, KieContainer> configGroupsContainers(DroolsConfigurationBean droolsConfigurationBean){

        Map<String, KieContainer> groupContainers = new HashMap<>();

        if (droolsConfigurationBean != null) {

            KieServices ks = KieServices.Factory.get();

            for (String groupName : droolsConfigurationBean.getListGroups()){

                DroolsConfigurationGroupBean groupConfigBean = droolsConfigurationBean.getGroups().get(groupName);

                KieContainer groupContainer = ks
                        .newKieContainer(
                           ks.newReleaseId(groupConfigBean.getGroupId(), groupConfigBean.getArtifactId(), groupConfigBean.getVersion())
                        );

                if (groupContainer != null) {

                    groupContainers.put(groupName, groupContainer);

                    // TODO Add Scanner to the container if it is required

                } else {

                    if (log.isDebugEnabled()){
                        log.debug("It was not possible to create a KieContainer for group {}", groupName);
                    }
                }

            }

        }


        return groupContainers;
    }

    public Map<String, KieContainer> getGroupContainers() {

        return groupContainers;
    }

    public KieContainer getGroupContainer(String groupName) {

        return groupContainers.get(groupName);
    }

    public Map<String, DroolsConfigurationGroupBean> getGroupConfigurations(){

        return groupConfigurations;
    }

    public DroolsConfigurationGroupBean getGroupConfiguration(String groupName) {

        return groupConfigurations.get(groupName);
    }

}
