package com.stratio.streaming.test.validator;

import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.exception.RequestValidationException;
import com.stratio.streaming.functions.validator.CassandraStreamColumnValidator;

public class CassandraColumnNameRegularExpressionValidatorTest extends BaseRegularExpressionValidatorTest {

    private CassandraStreamColumnValidator cassandraStreamColumnValidator;

    @Override
    public void setUp() {
        cassandraStreamColumnValidator = new CassandraStreamColumnValidator();
    }

    @Override
    public void test(StratioStreamingMessage message) throws RequestValidationException {
        cassandraStreamColumnValidator.validate(message);
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
