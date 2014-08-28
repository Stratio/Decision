package com.stratio.streaming.test.validator;

import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.exception.RequestValidationException;
import com.stratio.streaming.functions.validator.KafkaStreamNameValidator;

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
