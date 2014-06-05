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
package com.stratio.streaming.functions.validator;

import org.wso2.siddhi.core.SiddhiManager;

import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.exception.RequestValidationException;

public class StreamNotExistsValidation extends BaseSiddhiRequestValidation {

    private final static String STREAM_DOES_NOT_EXIST_MESSAGE = "Stream %s does not exists";

    public StreamNotExistsValidation(SiddhiManager sm) {
        super(sm);
    }

    @Override
    public void validate(StratioStreamingMessage request) throws RequestValidationException {
        if (request.getStreamName() != null && !"".equals(request.getStreamName())) {
            if (getSm().getStreamDefinition(request.getStreamName()) == null) {
                throw new RequestValidationException(REPLY_CODES.KO_STREAM_DOES_NOT_EXIST, String.format(
                        STREAM_DOES_NOT_EXIST_MESSAGE, request.getStreamName()));
            }
        }
    }
}
