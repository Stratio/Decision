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
import com.stratio.streaming.functions.validator.CassandraStreamNameValidator;

public class CassandraNameRegularExpressionValidatorTest extends BaseRegularExpressionValidatorTest {

    private CassandraStreamNameValidator cassandraStreamNameValidator;

    @Override
    public void setUp() {
        cassandraStreamNameValidator = new CassandraStreamNameValidator();
    }

    @Override
    public void test(StratioStreamingMessage message) throws RequestValidationException {
        cassandraStreamNameValidator.validate(message);
    }

    @Override
    public String[] getGoodStrings() {
        return new String[] { "a", "A", "Test34_pbk", "test_3_3_2_test" };
    }

    @Override
    public String[] getBadStrings() {
        return new String[] { "_", "1", "12322k", "`++++´´´", "_TEST", "2col" };
    }

}
