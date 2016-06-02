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

import java.io.Serializable;

/**
 * Utils class for implementing retrying in functions
 */
public class RetryStrategy implements Serializable {

    public static final int DEFAULT_RETRIES = 3;
    public static final long DEFAULT_WAIT_TIME_IN_MILLI = 2000;

    private int numberOfRetries;
    private int numberOfTriesLeft;
    private long timeToWait;

    public RetryStrategy() {
        this(DEFAULT_RETRIES, DEFAULT_WAIT_TIME_IN_MILLI);
    }

    public RetryStrategy(int numberOfRetries, long timeToWait) {
        this.numberOfRetries = numberOfRetries;
        numberOfTriesLeft = numberOfRetries;
        this.timeToWait = timeToWait;
    }

    /**
     * @return true if there are tries left
     */
    public boolean shouldRetry() {
        return numberOfTriesLeft > 0;
    }

    public void errorOccured() throws Exception {
        numberOfTriesLeft--;
        if (!shouldRetry()) {
            throw new Exception("Retry Failed: Total " + numberOfRetries
                    + " attempts made at interval " + getTimeToWait()
                    + "ms");
        }
        waitUntilNextTry();
    }

    public long getTimeToWait() {
        return timeToWait;
    }

    private void waitUntilNextTry() {
        try {
            Thread.sleep(getTimeToWait());
        } catch (InterruptedException ignored) {
        }
    }
}


