package com.stratio.decision.drools.configuration;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by jmartinmenor on 8/10/15.
 */
public class DroolsConfigurationBean {

    private Map<String, DroolsConfigurationGroupBean> groups;

    public Map<String, DroolsConfigurationGroupBean> getGroups() {
        return groups;
    }

    public void setGroups(Map<String, DroolsConfigurationGroupBean> groups) {
        this.groups = groups;
    }

    public String getQueryNameGroup(String group){
        return groups.get(group).getQueryName();
    }

    public List<String> getListGroups(){

        if (groups == null || groups.size() == 0 ){
            return null;
        }
        List<String> keys = new ArrayList<String>();
        Iterator<String> ite = groups.keySet().iterator();
        while(ite.hasNext()){
            keys.add(ite.next());
        }
        return keys;
    }

    public DroolsConfigurationGroupBean getGroup(String group){
        return groups.get(group);
    }
}
