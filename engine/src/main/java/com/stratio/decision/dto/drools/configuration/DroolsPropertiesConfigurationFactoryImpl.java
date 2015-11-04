package com.stratio.decision.dto.drools.configuration;

import static com.stratio.decision.dto.drools.configuration.ConfigurationConstants.BASE;
import static com.stratio.decision.dto.drools.configuration.ConfigurationConstants.INPUT_MODEL_LIST;
import static com.stratio.decision.dto.drools.configuration.ConfigurationConstants.QUERY_NAME;
import static com.stratio.decision.dto.drools.configuration.ConfigurationConstants.QUERY_RESULT_NAME;
import static com.stratio.decision.dto.drools.configuration.ConfigurationConstants.SESSION_NAME;
import static com.stratio.decision.dto.drools.configuration.ConfigurationConstants.URL_BASE;
import static com.stratio.decision.dto.drools.configuration.ConfigurationConstants.URL_WORKBECH;
import static com.stratio.decision.dto.drools.configuration.ConfigurationConstants.USER_NAME;
import static com.stratio.decision.dto.drools.configuration.ConfigurationConstants.VARIABLE;

import java.io.IOException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.stratio.decision.dto.drools.configuration.model.DroolsConfiguration;
import com.stratio.decision.dto.drools.configuration.model.DroolsConfigurationGroup;


/**
 * Created by jmartinmenor on 8/10/15.
 */
public class DroolsPropertiesConfigurationFactoryImpl implements DroolsConfigurationFactory{

    private static final String CONF_PATH = "/conf/droolsconf.properties";
    private List<String> configurationGroupsList;

    public DroolsConfiguration getConfiguration() throws IOException {

        DroolsConfiguration config = new DroolsConfiguration();

        Properties props = this.loadPropeties();

        config.setUrlBase(props.getProperty(URL_BASE));
        config.setUserName(props.getProperty(USER_NAME));
        config.setUserPass(props.getProperty(ConfigurationConstants.USER_PASS));
        config.setGroups(getConfigurationGroups(props));
        loadConfigurationGroupsData(config.getGroups(), props);

        return config;
    }

    private void loadConfigurationGroupsData(Map<String, DroolsConfigurationGroup> groups, Properties props) {

        Iterator<String> ite = groups.keySet().iterator();

        while(ite.hasNext()){
            String keyGroup = ite.next();
            DroolsConfigurationGroup g = groups.get(keyGroup);
            g.setSessionName(getPropetieName(props,keyGroup, SESSION_NAME));
            g.setQueryName(getPropetieName(props, keyGroup, QUERY_NAME));
            g.setQueryResultName(getPropetieName(props, keyGroup, QUERY_RESULT_NAME));
            g.setUrlWorkBench(getUrlList(getPropetieName(props, keyGroup, URL_WORKBECH)));
            g.setResultType(getPropetieName(props, keyGroup, ConfigurationConstants.RESULT_TYPE));
            g.setModelList(getModelList(props, keyGroup));
        }

    }

    private List<String> getUrlList(String urls) {
        List<String> l = null;
        if(urls != null) {
            String[] mla = urls.split("\\s*,\\s*");
            l = Arrays.asList(mla);
        }
        return l;
    }

    private List<String> getModelList(Properties props, String keyGroup) {
        String ml = getPropetieName(props, keyGroup, INPUT_MODEL_LIST);
        List<String> l = null;
        if(ml != null) {
            String[] mla = ml.split("\\s*,\\s*");
            l = Arrays.asList(mla);
        }
        return l;
    }

    private String getPropetieName(Properties prop, String keyGroup, String propertie) {
        return prop.getProperty(propertie.replace(VARIABLE, keyGroup));
    }

    private Properties loadPropeties() throws IOException {

        Properties props = new Properties();
        props.load(this.getClass().getResourceAsStream(CONF_PATH));

        return props;
    }

    public Map<String,DroolsConfigurationGroup> getConfigurationGroups(Properties props) {

        Map<String,DroolsConfigurationGroup> mGroups = new HashMap<String, DroolsConfigurationGroup>();

        Enumeration e = props.propertyNames();

        while(e.hasMoreElements()){
            String p = (String)e.nextElement();
            String[] pl = p.split("\\.");
            String base = pl[0];
            String key = pl[1];

            if(base.equals(BASE) && !mGroups.containsKey(key)){
                mGroups.put(key, new DroolsConfigurationGroup());
            }
        }

        return mGroups;
    }
}
