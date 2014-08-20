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
package com.stratio.streaming.commons.messages;

import java.util.List;

public class ListStreamsMessage {

    private Integer count;
    private Long timestamp;
    private List<StratioStreamingMessage> streams;

    public ListStreamsMessage() {
    }

    /**
     * @param count
     * @param timestamp
     * @param streams
     */
    public ListStreamsMessage(Integer count, Long timestamp, List<StratioStreamingMessage> streams) {
        super();
        this.count = count;
        this.timestamp = timestamp;
        this.streams = streams;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public List<StratioStreamingMessage> getStreams() {
        return streams;
    }

    public void setStreams(List<StratioStreamingMessage> streams) {
        this.streams = streams;
    }
}
