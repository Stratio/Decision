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
package com.stratio.decision.functions.validator;

import com.stratio.decision.functions.ActionBaseContext;
import com.stratio.decision.service.StreamOperationService;

public abstract class BaseSiddhiRequestValidation implements RequestValidation {

    private  StreamOperationService streamOperationService = null;

    public BaseSiddhiRequestValidation(){

    }

    public BaseSiddhiRequestValidation(StreamOperationService streamOperationService) {
        this.streamOperationService = streamOperationService;
    }

    public StreamOperationService getStreamOperationService() {

        if (streamOperationService == null) {

            streamOperationService = (StreamOperationService) ActionBaseContext.getInstance().getContext().getBean
                    ("streamOperationService");
        }

        return streamOperationService;
    }

}
