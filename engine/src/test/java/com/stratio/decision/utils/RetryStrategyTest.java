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
package com.stratio.decision.utils;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by aitor on 9/28/15.
 */
public class RetryStrategyTest {

    private RetryStrategy retry;

    @Before
    public void setUp() throws Exception {
        retry= new RetryStrategy();
    }

    @Test
    public void testShouldRetry() throws Exception {
        assertTrue("Expected value higher than zero", retry.shouldRetry());
    }

    @Test
    public void testErrorOccured() throws Exception {
        retry= new RetryStrategy(3, 100);
        retry.errorOccured();
        retry.errorOccured();

        Exception ex= null;
        try {
            retry.errorOccured();
        } catch (Exception e) {ex= e;}

        assertNotNull("Expected exception after some errors", ex);
    }

    @Test
    public void testGetTimeToWait() throws Exception {
        assertEquals("Unexpected time to wait value", 2000, retry.getTimeToWait());
        retry= new RetryStrategy(2, 1000);
        assertEquals("Unexpected time to wait value", 1000, retry.getTimeToWait());
    }
}