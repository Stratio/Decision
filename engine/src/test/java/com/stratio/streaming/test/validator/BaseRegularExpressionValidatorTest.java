package com.stratio.streaming.test.validator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stratio.streaming.commons.constants.ColumnType;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.exception.RequestValidationException;

public abstract class BaseRegularExpressionValidatorTest {

    @Before
    public void before() {
        this.setUp(getRegularExpression());
    }

    @Test
    public void goodMessagesTest() throws RequestValidationException {
        for (StratioStreamingMessage message : getGoodMessages()) {
            this.test(message);
        }
    }

    @Test(expected = RequestValidationException.class)
    public void badMessagesTest() throws RequestValidationException {
        for (StratioStreamingMessage message : getBadMessages()) {
            this.test(message);
        }
    }

    @Test
    public void mixedMessagesTest() {
        AtomicInteger count = new AtomicInteger(0);
        for (StratioStreamingMessage message : getMixedMessages()) {
            try {
                this.test(message);
            } catch (RequestValidationException e) {
                count.incrementAndGet();
            }
        }
        Assert.assertTrue(getBadStrings().length <= count.get());
    }

    public abstract void setUp(String regularExpression);

    public abstract void test(StratioStreamingMessage message) throws RequestValidationException;

    public abstract String[] getGoodStrings();

    public abstract String[] getBadStrings();

    public abstract String getRegularExpression();

    private List<StratioStreamingMessage> getGoodMessages() {
        return getMessages(getGoodStrings());
    }

    private List<StratioStreamingMessage> getBadMessages() {
        return getMessages(getBadStrings());
    }

    private List<StratioStreamingMessage> getMixedMessages() {
        List<String> allStrings = Arrays.asList(ArrayUtils.addAll(getGoodStrings(), getBadStrings()));
        Collections.shuffle(allStrings);
        return getMessages((String[]) allStrings.toArray());
    }

    private List<StratioStreamingMessage> getMessages(String[] names) {
        List<StratioStreamingMessage> result = new ArrayList<>();
        for (String streamName : names) {
            StratioStreamingMessage message = new StratioStreamingMessage();
            for (String colName : names) {
                message.addColumn(new ColumnNameTypeValue(colName, ColumnType.STRING, 0));
            }
            message.setStreamName(streamName);
            result.add(message);
        }
        return result;
    }
}
