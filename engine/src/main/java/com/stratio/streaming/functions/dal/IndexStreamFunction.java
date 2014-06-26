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
package com.stratio.streaming.functions.dal;

import java.util.Set;

import org.wso2.siddhi.core.SiddhiManager;

import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.constants.StreamAction;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.ActionBaseFunction;
import com.stratio.streaming.functions.validator.ActionEnabledValidation;
import com.stratio.streaming.functions.validator.RequestValidation;
import com.stratio.streaming.functions.validator.StreamNotExistsValidation;
import com.stratio.streaming.streams.StreamOperations;

public class IndexStreamFunction extends ActionBaseFunction {

    private static final long serialVersionUID = -689381870050478255L;

    private final String elasticSearchHost;
    private final int elasticSearchPort;

    public IndexStreamFunction(SiddhiManager siddhiManager, String zookeeperHost, String elasticSearchHost,
            int elasticSearchPort) {
        super(siddhiManager, zookeeperHost);
        this.elasticSearchHost = elasticSearchHost;
        this.elasticSearchPort = elasticSearchPort;
    }

    @Override
    protected String getStartOperationCommand() {
        return STREAM_OPERATIONS.ACTION.INDEX;
    }

    @Override
    protected String getStopOperationCommand() {
        return STREAM_OPERATIONS.ACTION.STOP_INDEX;
    }

    @Override
    protected boolean startAction(StratioStreamingMessage message) {
        StreamOperations.streamToIndexer(message, elasticSearchHost, elasticSearchPort, getSiddhiManager());
        return true;
    }

    @Override
    protected boolean stopAction(StratioStreamingMessage message) {
        StreamOperations.stopStreamToIndexer(message, getSiddhiManager());
        return true;
    }

    @Override
    protected void addStopRequestsValidations(Set<RequestValidation> validators) {
        validators.add(new StreamNotExistsValidation(getSiddhiManager()));
    }

    @Override
    protected void addStartRequestsValidations(Set<RequestValidation> validators) {
        validators.add(new ActionEnabledValidation(getSiddhiManager(), StreamAction.INDEXED,
                REPLY_CODES.KO_INDEX_STREAM_ALREADY_ENABLED));
        validators.add(new StreamNotExistsValidation(getSiddhiManager()));
    }

}
