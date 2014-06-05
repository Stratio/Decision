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
package com.stratio.streaming.callbacks;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import com.datastax.driver.core.querybuilder.Insert;
import com.google.common.collect.Lists;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.utils.SiddhiUtils;

public class StreamToMongoCallback extends StreamCallback implements MessageListener<String> {

    private static Logger logger = LoggerFactory.getLogger(StreamToCassandraCallback.class);

    private StreamDefinition streamDefinition;
    private String mongoNodesCluster;
    private Boolean running;

    public StreamToMongoCallback(StreamDefinition streamDefinition, String mongoNodesCluster) {
        this.streamDefinition = streamDefinition;
        this.mongoNodesCluster = mongoNodesCluster;
        running = Boolean.TRUE;
        init();
        logger.debug("Starting listener for stream " + streamDefinition.getStreamId());
    }

    private void init() {

    }

    @Override
    public void receive(Event[] events) {

        if (running) {

            List<StratioStreamingMessage> collected_events = Lists.newArrayList();

            for (Event e : events) {

                if (e instanceof InEvent) {
                    InEvent ie = (InEvent) e;
                    List<ColumnNameTypeValue> columns = Lists.newArrayList();
                    for (Attribute column : streamDefinition.getAttributeList()) {
                        if (ie.getData().length >= streamDefinition.getAttributePosition(column.getName()) + 1) {
                            columns.add(new ColumnNameTypeValue(column.getName(), SiddhiUtils.encodeSiddhiType(column
                                    .getType()), ie.getData(streamDefinition.getAttributePosition(column.getName()))));

                        }
                    }
                    collected_events.add(new StratioStreamingMessage(streamDefinition.getStreamId(), ie.getTimeStamp(),
                            columns));
                }
            }
            persistEventsToMongo(collected_events);
        }

    }

    private void persistEventsToMongo(List<StratioStreamingMessage> collected_events) {

        List<Insert> statements = Lists.newArrayList();

        for (StratioStreamingMessage event : collected_events) {
            // TODO
        }

        for (Insert statement : statements) {
            // TODO
        }

    }

    private void shutdownCallback() {
        if (running) {
            // TODO
        }
    }

    @Override
    public void onMessage(Message<String> message) {
        if (running) {
            if (message.getMessageObject().equalsIgnoreCase(streamDefinition.getStreamId())
                    || message.getMessageObject().equalsIgnoreCase("*")) {
                shutdownCallback();
                running = Boolean.FALSE;
                logger.debug("Shutting down save2mongo for stream " + streamDefinition.getStreamId());
            }
        }

    }

}
