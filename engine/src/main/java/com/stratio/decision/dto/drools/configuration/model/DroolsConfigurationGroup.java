package com.stratio.decision.dto.drools.configuration.model;

import java.util.List;

/**
 * Created by jmartinmenor on 8/10/15.
 */
public class DroolsConfigurationGroup {

    private String sessionName;
    private List<String> urlWorkBench;
    private String queryName;
    private List<String> modelList;
    private String queryResultName;
    private String resultType;

    public String getSessionName() {
        return sessionName;
    }

    public void setSessionName(String sessionName) {
        this.sessionName = sessionName;
    }

    public List<String> getUrlWorkBench() {
        return urlWorkBench;
    }

    public void setUrlWorkBench(List<String> urlWorkBench) {
        this.urlWorkBench = urlWorkBench;
    }

    public String getQueryName() {
        return queryName;
    }

    public void setQueryName(String queryName) {
        this.queryName = queryName;
    }

    public List<String> getModelList() {
        return modelList;
    }

    public void setModelList(List<String> modelList) {
        this.modelList = modelList;
    }

    public String getResultType() {
        return resultType;
    }

    public void setResultType(String resultType) {
        this.resultType = resultType;
    }

    public String getQueryResultName() {
        return queryResultName;
    }

    public void setQueryResultName(String queryResultName) {
        this.queryResultName = queryResultName;
    }
}
