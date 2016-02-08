package com.stratio.decision.streams;

import java.io.Serializable;
import java.util.Map;

import com.stratio.decision.commons.constants.EngineActionType;

/**
 * Created by josepablofernandez on 3/12/15.
 */

public class EngineActionDTO implements Serializable {


    private EngineActionType engineActionType;
    private Map<String, Object> engineActionParameters;
    private String engineActionQueryId;

    public EngineActionDTO(EngineActionType engineActionType, Map<String, Object> engineActionParameters, String
            engineActionQueryId) {

        this.engineActionType = engineActionType;
        this.engineActionParameters = engineActionParameters;
        this.engineActionQueryId = engineActionQueryId;
    }


    public EngineActionType getEngineActionType() {
        return engineActionType;
    }

    public void setEngineActionType(EngineActionType engineActionType) {
        this.engineActionType = engineActionType;
    }

    public Map<String, Object> getEngineActionParameters() {
        return engineActionParameters;
    }

    public void setEngineActionParameters(Map<String, Object> engineActionParameters) {
        this.engineActionParameters = engineActionParameters;
    }

    public String getEngineActionQueryId() {
        return engineActionQueryId;
    }

    public void setEngineActionQueryId(String engineActionQueryId) {
        this.engineActionQueryId = engineActionQueryId;
    }
}
