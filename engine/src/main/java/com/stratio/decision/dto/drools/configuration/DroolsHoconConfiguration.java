package com.stratio.decision.dto.drools.configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.stratio.decision.configuration.ConfigurationContext;
import com.stratio.decision.dto.drools.configuration.model.DroolsConfiguration;
import com.stratio.decision.dto.drools.configuration.model.DroolsConfigurationGroup;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Created by jmartinmenor on 8/10/15.
 */
public class DroolsHoconConfiguration {

    private static Config droolsConfig;

    public static DroolsConfiguration getConfiguration(ConfigurationContext configurationContext) throws IOException {

        droolsConfig= ConfigFactory.load("drools");
        DroolsConfiguration config = new DroolsConfiguration();

        config.setHost(configurationContext.getDroolsHost());
        config.setUsername(configurationContext.getDroolsUsername());
        config.setPassword(configurationContext.getDroolsPassword());
        config.setBatchSize(configurationContext.getDroolsBatchSize());
        config.setMappingLibraryDir(configurationContext.getDroolsMappingLibraryDir());

        config.setGroups(getDroolsConfigurationGroup(droolsConfig));

        return config;
    }

    private static Map<String, DroolsConfigurationGroup> getDroolsConfigurationGroup(Config droolsConfig)  {
        Map<String, DroolsConfigurationGroup> groups= new HashMap<>();
        List<String> groupNames= droolsConfig.getStringList("");
        for (String groupName: groupNames) {
            DroolsConfigurationGroup g= new DroolsConfigurationGroup();
            g.setSessionName(droolsConfig.getString(groupName + ".sessionName"));
            g.setGroupBatchSize(droolsConfig.getInt(groupName + ".groupBatchSize"));
            g.setGroupId(droolsConfig.getString(groupName + ".groupId"));
            g.setArtifactId(droolsConfig.getString(groupName + ".artifactId"));
            g.setVersion(droolsConfig.getString(groupName + ".version"));
            g.setScanFrecuency(droolsConfig.getLong(groupName + ".scanFrecuency"));
            g.setQueryName(droolsConfig.getString(groupName + ".queryName"));
            g.setResultName(droolsConfig.getString(groupName + ".resultName"));
            g.setMappingFile(droolsConfig.getString(groupName + ".mappingFile"));
            groups.put(groupName, g);
        }
        return groups;
    }

}
