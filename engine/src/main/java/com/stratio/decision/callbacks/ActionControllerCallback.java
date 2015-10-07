/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.decision.callbacks;

import java.util.Set;

import org.wso2.siddhi.core.query.output.callback.QueryCallback;

import com.stratio.decision.commons.constants.StreamAction;

public abstract class ActionControllerCallback extends QueryCallback {

    protected final Set<StreamAction> activeActions;

    public ActionControllerCallback(Set<StreamAction> activeActions) {
        this.activeActions = activeActions;
    }

}
