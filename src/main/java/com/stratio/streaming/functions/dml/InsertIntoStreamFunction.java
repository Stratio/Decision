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
package com.stratio.streaming.functions.dml;

import java.util.Set;

import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.exception.AttributeNotExistException;

import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.dto.ActionCallbackDto;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.exception.RequestValidationException;
import com.stratio.streaming.functions.ActionBaseFunction;
import com.stratio.streaming.functions.validator.RequestValidation;
import com.stratio.streaming.functions.validator.StreamNotExistsValidation;
import com.stratio.streaming.utils.SiddhiUtils;

public class InsertIntoStreamFunction extends ActionBaseFunction {

    private static final long serialVersionUID = -2545263418772827277L;

    public InsertIntoStreamFunction(SiddhiManager siddhiManager, String zookeeperHost) {
        super(siddhiManager, zookeeperHost);
    }

    @Override
    protected String getStartOperationCommand() {
        return STREAM_OPERATIONS.MANIPULATION.INSERT;
    }

    @Override
    protected String getStopOperationCommand() {
        return null;
    }

    @Override
    protected boolean startAction(StratioStreamingMessage message) throws RequestValidationException {
        try {
            getSiddhiManager().getInputHandler(message.getStreamName()).send(
                    SiddhiUtils.getOrderedValues(getSiddhiManager().getStreamDefinition(message.getStreamName()),
                            message.getColumns()));
        } catch (AttributeNotExistException e) {
            throw new RequestValidationException(REPLY_CODES.KO_COLUMN_DOES_NOT_EXIST, e.getMessage());
        } catch (InterruptedException e) {
            throw new RequestValidationException(REPLY_CODES.KO_GENERAL_ERROR, e.getMessage());
        }
        return false;
    }

    @Override
    protected void ackStreamingOperation(StratioStreamingMessage message, ActionCallbackDto reply) throws Exception {
        log.debug("Overriding zookeeper inser action.Data: {}", reply);
    }

    @Override
    protected boolean stopAction(StratioStreamingMessage message) throws RequestValidationException {
        // nothing to do
        return true;
    }

    @Override
    protected void addStopRequestsValidations(Set<RequestValidation> validators) {
    }

    @Override
    protected void addStartRequestsValidations(Set<RequestValidation> validators) {
        validators.add(new StreamNotExistsValidation(getSiddhiManager()));
    }
}
