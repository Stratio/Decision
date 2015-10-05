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
package com.stratio.decision.unit.engine.validator;

import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.exception.RequestValidationException;
import com.stratio.decision.functions.validator.KafkaStreamNameValidator;

public class KafkaNameRegularExpressionValidatorTest extends BaseRegularExpressionValidatorTest {

    private KafkaStreamNameValidator kafkaStreamNameValidator;

    @Override
    public void setUp() {
        kafkaStreamNameValidator = new KafkaStreamNameValidator();
    }

    @Override
    public void test(StratioStreamingMessage message) throws RequestValidationException {
        kafkaStreamNameValidator.validate(message);
    }

    @Override
    public String[] getGoodStrings() {
        return new String[] { "tesT_234--234----", "test", "_test", "test_3_3_2_test", "-testasf", "2323easdas" };
    }

    @Override
    public String[] getBadStrings() {
        return new String[] { "?asdasda", "asdas?asd", ")asdasd", "`++++´´´", "asd9)Adass", "4352345\"·$\"·45" };
    }

}
