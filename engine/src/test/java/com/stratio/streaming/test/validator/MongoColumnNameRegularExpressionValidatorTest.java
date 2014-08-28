package com.stratio.streaming.test.validator;

import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.exception.RequestValidationException;
import com.stratio.streaming.functions.validator.MongoStreamColumnValidator;

public class MongoColumnNameRegularExpressionValidatorTest extends BaseRegularExpressionValidatorTest {

    private MongoStreamColumnValidator mongoStreamColumnValidator;

    @Override
    public void setUp() {
        mongoStreamColumnValidator = new MongoStreamColumnValidator();
    }

    @Override
    public void test(StratioStreamingMessage message) throws RequestValidationException {
        mongoStreamColumnValidator.validate(message);
    }

    @Override
    public String[] getGoodStrings() {
        return new String[] { "test_test$etstsdd", "&&&&", "$$$$", "\n\n\n" };
    }

    @Override
    public String[] getBadStrings() {
        return new String[] { "^test", "test^", "test\"test" };
    }

}
