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
import com.stratio.streaming.streams.StreamSharedStatus;

public class QueryExistsValidation extends BaseSiddhiRequestValidation {

    private final static String QUERY_ALREADY_EXISTS_MESSAGE = "Query in stream %s already exists";

    public QueryExistsValidation(SiddhiManager sm) {
        super(sm);
    }

    @Override
    public void validate(StratioStreamingMessage request) throws RequestValidationException {
        if (StreamSharedStatus.getStreamStatus(request.getStreamName(), getSm()) != null) {
            // TODO normalize query and create their hash to verify correctly if
            // this query exists
            if (StreamSharedStatus.getStreamStatus(request.getStreamName(), getSm()).getAddedQueries()
                    .containsValue(request.getRequest())) {
                throw new RequestValidationException(REPLY_CODES.KO_QUERY_ALREADY_EXISTS, String.format(
                        QUERY_ALREADY_EXISTS_MESSAGE, request.getStreamName()));
            }
        }
    }
}
