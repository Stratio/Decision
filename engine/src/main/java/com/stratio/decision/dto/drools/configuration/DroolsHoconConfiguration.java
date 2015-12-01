package com.stratio.decision.dto.drools.configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.stratio.decision.configuration.ConfigurationContext;
import com.stratio.decision.dto.drools.configuration.model.DroolsConfigurationBean;
import com.stratio.decision.dto.drools.configuration.model.DroolsConfigurationGroupBean;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Created by jmartinmenor on 8/10/15.
 */
public class DroolsHoconConfiguration {

    private static Config droolsConfig;

    public static DroolsConfigurationBean getConfiguration(ConfigurationContext configurationContext) throws IOException {

        droolsConfig= ConfigFactory.load("drools");
        DroolsConfigurationBean config = new DroolsConfigurationBean();

        config.setHost(configurationContext.getDroolsHost());
        config.setUsername(configurationContext.getDroolsUsername());
        config.setPassword(configurationContext.getDroolsPassword());
        config.setBatchSize(configurationContext.getDroolsBatchSize());
        config.setMappingLibraryDir(configurationContext.getDroolsMappingLibraryDir());

        config.setGroups(getDroolsConfigurationGroup(droolsConfig));

        return config;
    }

    private static Map<String, DroolsConfigurationGroupBean> getDroolsConfigurationGroup(Config droolsConfig)  {
        Map<String, DroolsConfigurationGroupBean> groups= new HashMap<>();
        List<String> groupNames= droolsConfig.getStringList("");
        for (String groupName: groupNames) {
            DroolsConfigurationGroupBean g= new DroolsConfigurationGroupBean();
            g.setSessionName(droolsConfig.getString(groupName + ".sessionName"));
            g.setGroupBatchSize(droolsConfig.getInt(groupName + ".groupBatchSize"));
            g.setGroupId(droolsConfig.getString(groupName + ".groupId"));
            g.setArtifactId(droolsConfig.getString(groupName + ".artifactId"));
            g.setVersion(droolsConfig.getString(groupName + ".version"));
            g.setScanFrequency(droolsConfig.getLong(groupName + ".scanFrecuency"));
            g.setQueryName(droolsConfig.getString(groupName + ".queryName"));
            g.setResultName(droolsConfig.getString(groupName + ".resultName"));
            g.setMappingFile(droolsConfig.getString(groupName + ".mappingFile"));
            groups.put(groupName, g);
        }
        return groups;
    }

}
