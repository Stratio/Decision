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
package com.stratio.streaming.unit.engine.validator;

import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.exception.RequestValidationException;
import com.stratio.streaming.functions.validator.MongoStreamNameValidator;

public class MongoNameRegularExpressionValidatorTest extends BaseRegularExpressionValidatorTest {

    private MongoStreamNameValidator mongoStreamNameValidator;

    @Override
    public void setUp() {
        mongoStreamNameValidator = new MongoStreamNameValidator();
    }

    @Override
    public void test(StratioStreamingMessage message) throws RequestValidationException {
        mongoStreamNameValidator.validate(message);
    }

    @Override
    public String[] getGoodStrings() {
        return new String[] { "test_test$etstsdd", "&&&&", "$$$$", "\n\n\n" };
    }

    @Override
    public String[] getBadStrings() {
        return new String[] { "*test", "test*", "test test", ">><<<<>", "_____|" };
    }

}
