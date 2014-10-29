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

import com.stratio.streaming.commons.constants.ReplyCode;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.exception.RequestValidationException;
import com.stratio.streaming.service.StreamOperationService;

public class StreamNotExistsValidation extends BaseSiddhiRequestValidation {

    private final static String STREAM_DOES_NOT_EXIST_MESSAGE = "Stream %s does not exists";

    public StreamNotExistsValidation(StreamOperationService streamOperationService) {
        super(streamOperationService);
    }

    @Override
    public void validate(StratioStreamingMessage request) throws RequestValidationException {
        if (request.getStreamName() != null && !"".equals(request.getStreamName())) {
            if (!getStreamOperationService().streamExist(request.getStreamName())) {
                throw new RequestValidationException(ReplyCode.KO_STREAM_DOES_NOT_EXIST.getCode(), String.format(
                        STREAM_DOES_NOT_EXIST_MESSAGE, request.getStreamName()));
            }
        }
    }
}
