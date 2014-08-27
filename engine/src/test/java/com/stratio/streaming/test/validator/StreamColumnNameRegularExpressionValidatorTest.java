package com.stratio.streaming.test.validator;

import com.stratio.streaming.commons.constants.StreamAction;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.exception.RequestValidationException;
import com.stratio.streaming.functions.validator.StreamColumnsByRegularExpressionValidator;

public class StreamColumnNameRegularExpressionValidatorTest extends BaseRegularExpressionValidatorTest {

    private StreamColumnsByRegularExpressionValidator streamColumnsByRegularExpressionValidator;

    @Override
    public void setUp(String regularExpression) {
        streamColumnsByRegularExpressionValidator = new StreamColumnsByRegularExpressionValidator(regularExpression,
                StreamAction.LISTEN);
    }

    @Override
    public void test(StratioStreamingMessage message) throws RequestValidationException {
        streamColumnsByRegularExpressionValidator.validate(message);
    }

    @Override
    public String[] getGoodStrings() {
        return new String[] { "a", "A", "Test34_pbk", "test_3_3_2_test" };
    }

    @Override
    public String[] getBadStrings() {
        return new String[] { "_", "1", "12322k", "`++++´´´", "_TEST", "2col" };
    }

    @Override
    public String getRegularExpression() {
        return "^[A-Z,a-z][\\w,_,0-9]*";
    }
}
