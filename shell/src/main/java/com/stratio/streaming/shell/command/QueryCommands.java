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
public class QueryCommands implements CommandMarker {
    @Autowired
    private StratioStreamingApiWrapper ssaw;

    @CliCommand(value = "add query", help = "create new query")
    public String create(
            @CliOption(key = {"stream"}, help = "Stream to attach query", mandatory = true, optionContext = "stream") final String streamName,
            @CliOption(key = {"definition"}, help = "CEP query definition", mandatory = true) final String query) {
        try {
            String queryId = ssaw.api().addQuery(streamName, query);
            return "Query ".concat(streamName).concat(" created correctly with id ".concat(queryId));

        } catch (StratioStreamingException e) {
            throw new StreamingShellException(e);
        }
    }

    @CliCommand(value = "remove query", help = "remove an existing query")
    public String drop(
            @CliOption(key = {"stream"}, help = "Stream to deattach query", mandatory = true, optionContext = "stream") final String streamName,
            @CliOption(key = {"id"}, help = "query id", mandatory = true) final String queryId) {
        try {
            ssaw.api().removeQuery(streamName, queryId);
            return "Query ".concat(streamName).concat(" dropped correctly with id ".concat(queryId));
        } catch (StratioStreamingException e) {
            throw new StreamingShellException(e);
        }
    }

}
