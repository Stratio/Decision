package com.stratio.decision.drools.configuration;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by jmartinmenor on 8/10/15.
 */
public class DroolsConfigurationBean {

    private String host;
    private String username;
    private String password;
    private int batchSize;
    private String mappingLibraryDir;

    private Map<String, DroolsConfigurationGroupBean> groups;

    //PoC fields
    private String pocStreamName;
    private String pocGroupName;

    public String getPocStreamName() {
        return pocStreamName;
    }

    public void setPocStreamName(String pocStreamName) {
        this.pocStreamName = pocStreamName;
    }

    //

    public String getPocGroupName() {
        return pocGroupName;
    }

    public void setPocGroupName(String pocGroupName) {
        this.pocGroupName = pocGroupName;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String userName) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getMappingLibraryDir() {
        return mappingLibraryDir;
    }

    public void setMappingLibraryDir(String mappingLibraryDir) {
        this.mappingLibraryDir = mappingLibraryDir;
    }


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
