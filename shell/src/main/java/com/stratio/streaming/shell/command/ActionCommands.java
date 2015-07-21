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
package com.stratio.streaming.shell.command;

import com.stratio.streaming.commons.exceptions.StratioStreamingException;
import com.stratio.streaming.shell.exception.StreamingShellException;
import com.stratio.streaming.shell.wrapper.StratioStreamingApiWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

@Component
public class ActionCommands implements CommandMarker {

    @Autowired
    private StratioStreamingApiWrapper ssaw;

    @CliCommand(value = "index start", help = "index stream events")
    public String indexStart(
            @CliOption(key = {"stream"}, help = "The stream name", mandatory = true, optionContext = "stream") final String streamName) {
        try {
            ssaw.api().indexStream(streamName);
            return "Stream ".concat(streamName).concat(" indexed correctly");
        } catch (StratioStreamingException e) {
            throw new StreamingShellException(e);
        }
    }

    @CliCommand(value = "index stop", help = "stop index stream events")
    public String indexStop(
            @CliOption(key = {"stream"}, help = "The stream name", mandatory = true, optionContext = "stream") final String streamName) {
        try {
            ssaw.api().stopIndexStream(streamName);
            return "Stream ".concat(streamName).concat(" unindexed correctly");
        } catch (StratioStreamingException e) {
            throw new StreamingShellException(e);
        }
    }

    @CliCommand(value = "save cassandra start", help = "start save to cassandra action")
    public String saveCassandraStart(
            @CliOption(key = {"stream"}, help = "The stream name", mandatory = true, optionContext = "stream") final String streamName) {
        try {
            ssaw.api().saveToCassandra(streamName);
            return "Stream ".concat(streamName).concat(" attached to cassandra correctly");
        } catch (StratioStreamingException e) {
            throw new StreamingShellException(e);
        }
    }

    @CliCommand(value = "save cassandra stop", help = "stop save to cassandra action")
    public String saveCassandraStop(
            @CliOption(key = {"stream"}, help = "The stream name", mandatory = true, optionContext = "stream") final String streamName) {
        try {
            ssaw.api().stopSaveToCassandra(streamName);
            return "Stream ".concat(streamName).concat(" de-attached from cassandra correctly");
        } catch (StratioStreamingException e) {
            throw new StreamingShellException(e);
        }
    }

    @CliCommand(value = "save mongo start", help = "start save to mongo action")
    public String saveMongoStart(
            @CliOption(key = {"stream"}, help = "The stream name", mandatory = true, optionContext = "stream") final String streamName) {
        try {
            ssaw.api().saveToMongo(streamName);
            return "Stream ".concat(streamName).concat(" attached to mongo correctly");
        } catch (StratioStreamingException e) {
            throw new StreamingShellException(e);
        }
    }

    @CliCommand(value = "save mongo stop", help = "stop save to mongo action")
    public String saveMongoStop(
            @CliOption(key = {"stream"}, help = "The stream name", mandatory = true, optionContext = "stream") final String streamName) {
        try {
            ssaw.api().stopSaveToMongo(streamName);
            return "Stream ".concat(streamName).concat(" de-attached from mongo correctly");
        } catch (StratioStreamingException e) {
            throw new StreamingShellException(e);
        }
    }

    @CliCommand(value = "listen start", help = "attach stream to kafka topic")
    public String listenStart(
            @CliOption(key = {"stream"}, help = "The stream name", mandatory = true, optionContext = "stream") final String streamName) {
        try {
            ssaw.api().listenStream(streamName);
            return "Stream ".concat(streamName).concat(" listened correctly");
        } catch (StratioStreamingException e) {
            throw new StreamingShellException(e);
        }
    }

    @CliCommand(value = "listen stop", help = "de-attach stream to kafka topic")
    public String listenStop(
            @CliOption(key = {"stream"}, help = "The stream name", mandatory = true, optionContext = "stream") final String streamName) {
        try {
            ssaw.api().stopListenStream(streamName);
            return "Stream ".concat(streamName).concat(" unlistened correctly");
        } catch (StratioStreamingException e) {
            throw new StreamingShellException(e);
        }
    }

    @CliCommand(value = "save rabbit start", help = "attach stream to Rabbit MQ")
    public String saveRabbitStart(
            @CliOption(key = {"stream"}, help = "The stream name", mandatory = true, optionContext = "stream") final String streamName) {
        try {
            ssaw.api().saveToRabbitMQ(streamName);
            return "Stream ".concat(streamName).concat(" attached to Rabbit MQ correctly");
        } catch (StratioStreamingException e) {
            throw new StreamingShellException(e);
        }
    }

    @CliCommand(value = "save rabbit stop", help = "de-attach stream to Rabbit MQ")
    public String saveRabbitStop(
            @CliOption(key = {"stream"}, help = "The stream name", mandatory = true, optionContext = "stream") final String streamName) {
        try {
            ssaw.api().stopSaveToRabbitMQ(streamName);
            return "Stream ".concat(streamName).concat(" de-attacched from Rabbit MQ correctly");
        } catch (StratioStreamingException e) {
            throw new StreamingShellException(e);
        }
    }

}
