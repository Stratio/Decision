/*******************************************************************************
 * Copyright 2014 Stratio
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.stratio.streaming.functions.dal;

import java.util.Set;

import org.wso2.siddhi.core.SiddhiManager;

import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.ActionBaseFunction;
import com.stratio.streaming.functions.validator.ActionEnabledValidation;
import com.stratio.streaming.functions.validator.RequestValidation;
import com.stratio.streaming.functions.validator.StreamExistsValidation;
import com.stratio.streaming.streams.StreamOperations;
import com.stratio.streaming.streams.StreamStatusDTO.StreamAction;

public class ListenStreamFunction extends ActionBaseFunction {
    private static final long serialVersionUID = 4566359991793310850L;

    private final String kafkaCluster;

    public ListenStreamFunction(SiddhiManager siddhiManager, String zookeeperHost, String kafkaCluster) {
        super(siddhiManager, zookeeperHost);
        this.kafkaCluster = kafkaCluster;
    }

    @Override
    protected String getStartOperationCommand() {
        return STREAM_OPERATIONS.ACTION.LISTEN;
    }

    @Override
    protected String getStopOperationCommand() {
        return STREAM_OPERATIONS.ACTION.STOP_LISTEN;
    }

    @Override
    protected boolean startAction(StratioStreamingMessage message) {
        StreamOperations.listenStream(message, kafkaCluster, getSiddhiManager());
        return true;
    }

    @Override
    protected boolean stopAction(StratioStreamingMessage message) {
        StreamOperations.stopListenStream(message, getSiddhiManager());
        return true;
    }

    @Override
    protected void addRequestsValidations(Set<RequestValidation> validators) {
        validators.add(new ActionEnabledValidation(getSiddhiManager(), StreamAction.LISTEN,
                REPLY_CODES.KO_LISTENER_ALREADY_EXISTS));
        validators.add(new StreamExistsValidation(getSiddhiManager()));
    }
}
