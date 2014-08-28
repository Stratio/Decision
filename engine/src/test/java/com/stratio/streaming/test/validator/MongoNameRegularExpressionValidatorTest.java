package com.stratio.streaming.test.validator;

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
