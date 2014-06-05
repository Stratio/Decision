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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.ShellException;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.shell.support.table.TableRenderer;
import org.springframework.stereotype.Component;

import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.commons.exceptions.StratioAPISecurityException;
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException;
import com.stratio.streaming.commons.exceptions.StratioEngineStatusException;
import com.stratio.streaming.commons.exceptions.StratioStreamingException;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.streams.StratioStream;
import com.stratio.streaming.shell.converter.wrapper.ColumnNameTypeList;
import com.stratio.streaming.shell.converter.wrapper.ColumnNameValueList;
import com.stratio.streaming.shell.dao.CachedStreamsDAO;
import com.stratio.streaming.shell.renderer.Renderer;

@Component
public class StreamCommands implements CommandMarker {

    @Autowired
    private IStratioStreamingAPI stratioStreamingApi;

    @Autowired
    private CachedStreamsDAO cachedStreamsDAO;

    @Autowired
    private Renderer<List<StratioStream>> stratioStreamRenderer;

    @CliCommand(value = "list", help = "list all streams into engine")
    public String list() {
        try {
            List<StratioStream> streams = cachedStreamsDAO.listUncachedStreams();
            return stratioStreamRenderer.render(streams);
        } catch (StratioStreamingException e) {
            throw new ShellException(e);
        }
    }

    @CliCommand(value = "columns", help = "list all streams querys into engine")
    public String listQuerys(
            @CliOption(key = { "stream" }, help = "The stream name", mandatory = true, optionContext = "stream") final String streamName) {
        // TODO create new renderer to render this table
        try {

            List<ColumnNameTypeValue> columnsValues = stratioStreamingApi.columnsFromStream(streamName);

            List<String> columns = Arrays.asList("Column", "Type");
            List<Map<String, Object>> data = new ArrayList<>();

            for (ColumnNameTypeValue columnValue : columnsValues) {
                Map<String, Object> row = new HashMap<>();
                row.put("Column", columnValue.getColumn());
                row.put("Type", columnValue.getType());

                data.add(row);
            }

            return TableRenderer.renderMapDataAsTable(data, columns);
        } catch (StratioEngineOperationException e) {
            throw new ShellException(e);
        }
    }

    @CliCommand(value = "create", help = "create new stream")
    public String create(
            @CliOption(key = { "stream" }, help = "The new stream name", mandatory = true) final String streamName,
            @CliOption(key = { "definition" }, help = "Stream definition. Comma seaparated name.type fields. Example: 'id.int,name.string,age.int,date.long", mandatory = true) final ColumnNameTypeList columns) {
        try {
            cachedStreamsDAO.newStream(streamName, columns);
            return "Stream ".concat(streamName).concat(" created correctly");

        } catch (StratioEngineStatusException | StratioAPISecurityException | StratioEngineOperationException e) {
            throw new ShellException(e);
        }
    }

    @CliCommand(value = "drop", help = "drop existing stream")
    public String drop(
            @CliOption(key = { "stream" }, help = "The stream name", mandatory = true, optionContext = "stream") final String streamName) {
        try {
            cachedStreamsDAO.dropStream(streamName);
            return "Stream ".concat(streamName).concat(" dropped correctly");
        } catch (StratioEngineStatusException | StratioAPISecurityException | StratioEngineOperationException e) {
            throw new ShellException(e);
        }
    }

    @CliCommand(value = "alter", help = "alter existing stream")
    public String alter(
            @CliOption(key = { "stream" }, help = "The stream name", mandatory = true, optionContext = "stream") final String streamName,
            @CliOption(key = { "definition" }, help = "Stream definition to add. Comma seaparated name.type fields. Example: 'id.int,name.string,age.int,date.long", mandatory = true) final ColumnNameTypeList columns) {
        try {
            stratioStreamingApi.alterStream(streamName, columns);
            return "Stream ".concat(streamName).concat(" altered correctly");

        } catch (StratioEngineStatusException | StratioAPISecurityException | StratioEngineOperationException e) {
            throw new ShellException(e);
        }
    }

    @CliCommand(value = "insert", help = "insert events into existing stream")
    public String insert(
            @CliOption(key = { "stream" }, help = "The stream name", mandatory = true, optionContext = "stream") final String streamName,
            @CliOption(key = { "values" }, help = "Values to add. Comma seaparated name.value fields. Example: 'id.345,name.Test test test,age.26,date.1401198535", mandatory = true) final ColumnNameValueList columns) {
        try {
            stratioStreamingApi.insertData(streamName, columns);
            return "Added an event to stream ".concat(streamName).concat(" correctly");
        } catch (StratioEngineStatusException | StratioAPISecurityException e) {
            throw new ShellException(e);
        }
    }
}
