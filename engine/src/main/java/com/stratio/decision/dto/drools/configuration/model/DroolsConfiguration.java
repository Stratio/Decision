package com.stratio.decision.dto.drools.configuration.model;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by jmartinmenor on 8/10/15.
 */
public class DroolsConfiguration {

    private String urlBase;
    private String userName;
    private String userPass;
    private Map<String,DroolsConfigurationGroup> groups;

    public String getUrlBase() {
        return urlBase;
    }

    public void setUrlBase(String urlBase) {
        this.urlBase = urlBase;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getUserPass() {
        return userPass;
    }

    public void setUserPass(String userPass) {
        this.userPass = userPass;
    }

    public Map<String, DroolsConfigurationGroup> getGroups() {
        return groups;
    }

    public void setGroups(
            Map<String, DroolsConfigurationGroup> groups) {
        this.groups = groups;
    }

    public String getUrlWorkbenchGroup(String group) {
        return urlBase+ File.separator + groups.get(group).getUrlWorkBench();
    }

    public List<String> getModelListGroup(String group){
        return groups.get(group).getModelList();
    }

    public String getQueryNameGroup(String group){
        return groups.get(group).getQueryName();
    }

    public List<String> getListGroups(){
        List<String> keys = new ArrayList<String>();
        Iterator<String> ite = groups.keySet().iterator();
        while(ite.hasNext()){
            keys.add(ite.next());
        }
        return keys;
    }

    public String getResultTypeGroup(String group){
        return groups.get(group).getResultType();
    }

    public DroolsConfigurationGroup getGroup(String group){
        return groups.get(group);
    }
}
