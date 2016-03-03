/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
