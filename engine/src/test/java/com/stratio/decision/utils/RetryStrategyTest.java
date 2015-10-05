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