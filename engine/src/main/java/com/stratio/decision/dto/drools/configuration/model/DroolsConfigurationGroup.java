package com.stratio.decision.dto.drools.configuration.model;

/**
 * Created by jmartinmenor on 8/10/15.
 */
public class DroolsConfigurationGroup {

    private String sessionName;
    private int groupBatchSize;
    private String groupId;
    private String artifactId;
    private String version;
    private long scanFrecuency;
    private String queryName;
    private String resultName;
    private String mappingFile;

    //private List<String> urlWorkBench;
    //private List<String> modelList;
    //private String queryResultName;
    //private String resultType;

    public String getSessionName() {
        return sessionName;
    }

    public void setSessionName(String sessionName) {
        this.sessionName = sessionName;
    }

    public int getGroupBatchSize() {
        return groupBatchSize;
    }

    public void setGroupBatchSize(int groupBatchSize) {
        this.groupBatchSize = groupBatchSize;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getArtifactId() {
        return artifactId;
    }

    public void setArtifactId(String artifactId) {
        this.artifactId = artifactId;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public long getScanFrecuency() {
        return scanFrecuency;
    }

    public void setScanFrecuency(long scanFrecuency) {
        this.scanFrecuency = scanFrecuency;
    }

    public String getQueryName() {
        return queryName;
    }

    public void setQueryName(String queryName) {
        this.queryName = queryName;
    }

    public String getResultName() {
        return resultName;
    }

    public void setResultName(String resultName) {
        this.resultName = resultName;
    }

    public String getMappingFile() {
        return mappingFile;
    }

    public void setMappingFile(String mappingFile) {
        this.mappingFile = mappingFile;
    }

    /*
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

    public List<String> getUrlWorkBench() {
        return urlWorkBench;
      }

    public void setUrlWorkBench(List<String> urlWorkBench) {
        this.urlWorkBench = urlWorkBench;

    }
    */
}
