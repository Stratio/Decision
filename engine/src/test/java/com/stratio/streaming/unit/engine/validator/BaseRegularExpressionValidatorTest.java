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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

import com.stratio.streaming.commons.constants.ColumnType;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.exception.RequestValidationException;

public abstract class BaseRegularExpressionValidatorTest {

    @Before
    public void before() {
        this.setUp();
    }

    @Ignore
    @Test
    public void goodMessagesTest() throws RequestValidationException {
        for (StratioStreamingMessage message : getGoodMessages()) {
            this.test(message);
        }
    }

    @Ignore
    @Test(expected = RequestValidationException.class)
    public void badMessagesTest() throws RequestValidationException {
        for (StratioStreamingMessage message : getBadMessages()) {
            this.test(message);
        }
    }

    @Ignore
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

    public abstract void setUp();

    public abstract void test(StratioStreamingMessage message) throws RequestValidationException;

    public abstract String[] getGoodStrings();

    public abstract String[] getBadStrings();

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
