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
package com.stratio.streaming.functions;

import com.google.gson.JsonObject;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.stratio.streaming.commons.constants.STREAMING;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SaveToRabbitMQActionExecutionFunction extends BaseActionExecutionFunction {

    private static final long serialVersionUID = 5557080301384923517L;

    private static Logger log = LoggerFactory.getLogger(SaveToRabbitMQActionExecutionFunction.class);

    private final String host;
    private final String user;
    private final String password;

    private Connection connection;
    private Channel channel;

    public SaveToRabbitMQActionExecutionFunction(String host, String user, String password) {
        this.host = host;
        this.user = user;
        this.password = password;
    }

    @Override
    public void process(Iterable<StratioStreamingMessage> messages) throws Exception {
        for(StratioStreamingMessage event: messages) {
            JsonObject rabbitEvent = buildRabbitMQEvent(event);
            getChannel().queueDeclare(event.getStreamName(), true, false, false, null);
            channel.queueBind(event.getStreamName(), STREAMING.STREAMING_KEYSPACE_NAME, event.getStreamName());
            channel.basicPublish(STREAMING.STREAMING_KEYSPACE_NAME, event.getStreamName(), null,
                    rabbitEvent.toString().getBytes());
        }
    }

    private Connection getConnection() throws Exception {
        if (connection == null) {
            if (host == null) {
                throw new IllegalArgumentException("Configured RabbitMQ must not be null");
            }
            ConnectionFactory factory = new ConnectionFactory();
            String[] hostElements = host.split(":");
            if(user != null && password != null) {
                factory.setUsername(user);
                factory.setPassword(password);
            }
            factory.setHost(hostElements[0]);
            factory.setPort(Integer.parseInt(hostElements[1]));
            connection = factory.newConnection();
        }
        return connection;
    }

    private Channel getChannel() throws Exception {
        if (channel == null) {
            channel = getConnection().createChannel();
            channel.exchangeDeclare(STREAMING.STREAMING_KEYSPACE_NAME, "direct", true);
        }
        return channel;
    }

    private JsonObject buildRabbitMQEvent(StratioStreamingMessage event) {
        JsonObject object = new JsonObject();
        object.addProperty(TIMESTAMP_FIELD, event.getTimestamp());
        for(ColumnNameTypeValue column: event.getColumns()) {
            object.addProperty(column.getColumn(), column.getValue().toString());
        }
        return object;
    }

}
