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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.ShellException;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.commons.exceptions.StratioAPISecurityException;
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException;
import com.stratio.streaming.commons.exceptions.StratioEngineStatusException;

@Component
public class QueryCommands implements CommandMarker {
    @Autowired
    private IStratioStreamingAPI stratioStreamingApi;

    @CliCommand(value = "add query", help = "create new query")
    public String create(
            @CliOption(key = { "stream" }, help = "Stream to attach query", mandatory = true, optionContext = "stream") final String streamName,
            @CliOption(key = { "definition" }, help = "CEP query definition", mandatory = true) final String query) {
        try {
            String queryId = stratioStreamingApi.addQuery(streamName, query);
            return "Query ".concat(streamName).concat(" created correctly with id ".concat(queryId));

        } catch (StratioEngineStatusException | StratioAPISecurityException | StratioEngineOperationException e) {
            throw new ShellException(e);
        }
    }

    @CliCommand(value = "remove query", help = "remove an existing query")
    public String drop(
            @CliOption(key = { "stream" }, help = "Stream to deattach query", mandatory = true, optionContext = "stream") final String streamName,
            @CliOption(key = { "id" }, help = "query id", mandatory = true) final String queryId) {
        try {
            stratioStreamingApi.removeQuery(streamName, queryId);
            return "Query ".concat(streamName).concat(" dropped correctly with id ".concat(queryId));
        } catch (StratioEngineStatusException | StratioEngineOperationException e) {
            throw new ShellException(e);
        }
    }

}
