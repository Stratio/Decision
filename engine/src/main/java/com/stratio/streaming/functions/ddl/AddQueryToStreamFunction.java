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
package com.stratio.streaming.functions.ddl;

import java.util.Set;

import org.wso2.siddhi.core.exception.DifferentDefinitionAlreadyExistException;
import org.wso2.siddhi.query.api.exception.MalformedAttributeException;
import org.wso2.siddhi.query.api.exception.SourceNotExistException;
import org.wso2.siddhi.query.compiler.exception.SiddhiPraserException;

import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.exception.RequestValidationException;
import com.stratio.streaming.functions.ActionBaseFunction;
import com.stratio.streaming.functions.validator.QueryExistsValidation;
import com.stratio.streaming.functions.validator.QueryNotExistsValidation;
import com.stratio.streaming.functions.validator.RequestValidation;
import com.stratio.streaming.functions.validator.StreamNotExistsValidation;
import com.stratio.streaming.service.StreamOperationService;

public class AddQueryToStreamFunction extends ActionBaseFunction {

    private static final long serialVersionUID = -9194965881511759849L;

    public AddQueryToStreamFunction(StreamOperationService streamOperationService, String zookeeperHost) {
        super(streamOperationService, zookeeperHost);
    }

    @Override
    protected String getStartOperationCommand() {
        return STREAM_OPERATIONS.DEFINITION.ADD_QUERY;
    }

    @Override
    protected String getStopOperationCommand() {
        return STREAM_OPERATIONS.DEFINITION.REMOVE_QUERY;
    }

    @Override
    protected boolean startAction(StratioStreamingMessage message) throws RequestValidationException {
        try {
            getStreamOperationService().addQuery(message.getStreamName(), message.getRequest());
        } catch (MalformedAttributeException e) {
            throw new RequestValidationException(REPLY_CODES.KO_COLUMN_DOES_NOT_EXIST, e.getMessage());
        } catch (SourceNotExistException e) {
            throw new RequestValidationException(REPLY_CODES.KO_SOURCE_STREAM_DOES_NOT_EXIST, e.getMessage());
        } catch (SiddhiPraserException e) {
            throw new RequestValidationException(REPLY_CODES.KO_PARSER_ERROR, e.getMessage());
        } catch (DifferentDefinitionAlreadyExistException e) {
            throw new RequestValidationException(REPLY_CODES.KO_OUTPUTSTREAM_EXISTS_AND_DEFINITION_IS_DIFFERENT,
                    e.getMessage());
        }
        return true;
    }

    @Override
    protected boolean stopAction(StratioStreamingMessage message) {
        getStreamOperationService().removeQuery(message.getRequest(), message.getStreamName());
        return true;
    }

    @Override
    protected void addStopRequestsValidations(Set<RequestValidation> validators) {
        validators.add(new StreamNotExistsValidation(getStreamOperationService()));
        validators.add(new QueryNotExistsValidation(getStreamOperationService()));
    }

    @Override
    protected void addStartRequestsValidations(Set<RequestValidation> validators) {
        validators.add(new QueryExistsValidation(getStreamOperationService()));
        validators.add(new StreamNotExistsValidation(getStreamOperationService()));
    }

}
