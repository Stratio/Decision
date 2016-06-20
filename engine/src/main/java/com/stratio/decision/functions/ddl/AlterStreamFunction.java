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
import com.stratio.decision.exception.ServiceException;
import com.stratio.decision.functions.ActionBaseFunction;
import com.stratio.decision.functions.validator.RequestValidation;
import com.stratio.decision.functions.validator.StreamNotExistsValidation;
import com.stratio.decision.service.StreamOperationService;
import org.wso2.siddhi.query.compiler.exception.SiddhiParserException;

import java.util.Set;

public class AlterStreamFunction extends ActionBaseFunction {

    private static final long serialVersionUID = -4776676321715167213L;

    public AlterStreamFunction(StreamOperationService streamOperationService, String zookeeperHost) {
        super(streamOperationService, zookeeperHost);
    }

    @Override
    protected String getStartOperationCommand() {
        return STREAM_OPERATIONS.DEFINITION.ALTER;
    }

    @Override
    protected String getStopOperationCommand() {
        return null;
    }

    @Override
    protected boolean startAction(StratioStreamingMessage message) throws RequestValidationException {
        try {
            int addedColumns = getStreamOperationService().enlargeStream(message.getStreamName(), message.getColumns());
            log.debug("Added {} columns to stream {}", addedColumns, message.getStreamName());
        } catch (SiddhiParserException e) {
            throw new RequestValidationException(ReplyCode.KO_PARSER_ERROR.getCode(), e);
        } catch (ServiceException e) {
            throw new RequestValidationException(ReplyCode.KO_COLUMN_ALREADY_EXISTS.getCode(), e);
        }
        return true;
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
        validators.add(new StreamNotExistsValidation());
    }
}
