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
package com.stratio.decision.functions.ddl;

import com.stratio.decision.commons.constants.ReplyCode;
import com.stratio.decision.commons.constants.STREAM_OPERATIONS;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.exception.RequestValidationException;
import com.stratio.decision.functions.ActionBaseFunction;
import com.stratio.decision.functions.validator.*;
import com.stratio.decision.service.StreamOperationService;
import org.wso2.siddhi.core.exception.DifferentDefinitionAlreadyExistException;
import org.wso2.siddhi.query.api.exception.MalformedAttributeException;
import org.wso2.siddhi.query.api.exception.SourceNotExistException;
import org.wso2.siddhi.query.compiler.exception.SiddhiParserException;

import java.util.Set;

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
            throw new RequestValidationException(ReplyCode.KO_COLUMN_DOES_NOT_EXIST.getCode(), e);
        } catch (SourceNotExistException e) {
            throw new RequestValidationException(ReplyCode.KO_SOURCE_STREAM_DOES_NOT_EXIST.getCode(), e);
        } catch (SiddhiParserException e) {
            throw new RequestValidationException(ReplyCode.KO_PARSER_ERROR.getCode(), e);
        } catch (DifferentDefinitionAlreadyExistException e) {
            throw new RequestValidationException(
                    ReplyCode.KO_OUTPUTSTREAM_EXISTS_AND_DEFINITION_IS_DIFFERENT.getCode(), e);
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
//        validators.add(new StreamNotExistsValidation(getStreamOperationService()));
//        validators.add(new QueryNotExistsValidation(getStreamOperationService()));
//        validators.add(new UserDefinedStreamValidation(getStreamOperationService()));

        validators.add(new StreamNotExistsValidation());
        validators.add(new QueryNotExistsValidation());
        validators.add(new UserDefinedStreamValidation());
    }

    @Override
    protected void addStartRequestsValidations(Set<RequestValidation> validators) {
//        validators.add(new QueryExistsValidation(getStreamOperationService()));
//        validators.add(new StreamNotExistsValidation(getStreamOperationService()));

        validators.add(new QueryExistsValidation());
        validators.add(new StreamNotExistsValidation());
    }

}
