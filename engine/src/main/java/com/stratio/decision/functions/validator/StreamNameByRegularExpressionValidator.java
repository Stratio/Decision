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
package com.stratio.decision.functions.validator;

import com.stratio.decision.commons.constants.StreamAction;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.exception.RequestValidationException;

public class StreamNameByRegularExpressionValidator extends BaseRegularExpressionValidator {

    private final StreamAction streamAction;

    public StreamNameByRegularExpressionValidator(String pattern, StreamAction streamAction) {
        super(pattern);
        this.streamAction = streamAction;
    }

    @Override
    public void validate(StratioStreamingMessage request) throws RequestValidationException {
        stringMatch(request.getStreamName());
    }

    @Override
    public String getHumanReadableErrorMessage(String stringValidated) {
        return String.format("Stream name %s is not compatible with %s action.", stringValidated, streamAction);
    }

}
