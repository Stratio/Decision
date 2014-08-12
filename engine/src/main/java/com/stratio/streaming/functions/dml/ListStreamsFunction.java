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
package com.stratio.streaming.functions.dml;

import java.util.List;
import java.util.Set;

import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.messages.ListStreamsMessage;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.exception.RequestValidationException;
import com.stratio.streaming.functions.ActionBaseFunction;
import com.stratio.streaming.functions.validator.RequestValidation;
import com.stratio.streaming.functions.validator.StreamAllowedValidation;
import com.stratio.streaming.service.StreamOperationService;
import com.stratio.streaming.utils.ZKUtils;

public class ListStreamsFunction extends ActionBaseFunction {

    private static final long serialVersionUID = 3580834398296372380L;

    public ListStreamsFunction(StreamOperationService streamOperationService, String zookeeperHost) {
        super(streamOperationService, zookeeperHost);
    }

    @Override
    protected String getStartOperationCommand() {
        return STREAM_OPERATIONS.MANIPULATION.LIST;
    }

    @Override
    protected String getStopOperationCommand() {
        return null;
    }

    @Override
    protected boolean startAction(StratioStreamingMessage message) throws RequestValidationException {
        List<StratioStreamingMessage> existingStreams = getStreamOperationService().list();
        try {
            ZKUtils.getZKUtils(getZookeeperHost()).createZNodeJsonReply(message,
                    new ListStreamsMessage(existingStreams.size(), System.currentTimeMillis(), existingStreams));
        } catch (Exception e) {
            throw new RequestValidationException(REPLY_CODES.KO_GENERAL_ERROR, e);
        }
        return false;
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
        validators.add(new StreamAllowedValidation());
    }
}
