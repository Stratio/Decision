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
import com.stratio.streaming.messaging.ColumnNameType;
import com.stratio.streaming.shell.dao.CachedStreamsDAO;

@Component
public class StreamCommands implements CommandMarker {

    @Autowired
    private IStratioStreamingAPI stratioStreamingApi;

    @Autowired
    private CachedStreamsDAO cachedStreamsDAO;

    @CliCommand(value = "list", help = "list all streams into engine")
    public String list() {
        try {
            // TODO refactor. Use constants to define columns
            List<StratioStream> streams = cachedStreamsDAO.listUncachedStreams();

            List<String> columns = Arrays.asList("Name", "User defined", "Queries", "Elements");
            List<Map<String, Object>> data = new ArrayList<>();

            for (StratioStream stratioStream : streams) {
                Map<String, Object> row = new HashMap<>();
                row.put("Name", stratioStream.getStreamName());
                row.put("User defined", stratioStream.getUserDefined());
                row.put("Queries", stratioStream.getQueries().size());
                row.put("Elements", stratioStream.getColumns().size());

                data.add(row);
            }

            return TableRenderer.renderMapDataAsTable(data, columns);
        } catch (StratioStreamingException e) {
            throw new ShellException(e);
        }
    }

    @CliCommand(value = "columns", help = "list all streams querys into engine")
    public String listQuerys(
            @CliOption(key = { "stream" }, help = "The stream name", mandatory = true, optionContext = "stream") final String streamName) {

        // TODO refactor. Use constants to define columns
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
    }

    @CliCommand(value = "create", help = "create new stream")
    public String create(
            @CliOption(key = { "stream" }, help = "The new stream name", mandatory = true) final String streamName,
            @CliOption(key = { "definition" }, help = "Stream definition. Comma seaparated name.type fields. Example: 'id.int,name.string,age.int,date.long", mandatory = true) final List<ColumnNameType> columns) {
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
            @CliOption(key = { "definition" }, help = "Stream definition to add. Comma seaparated name.type fields. Example: 'id.int,name.string,age.int,date.long", mandatory = true) final List<ColumnNameType> columns) {
        try {
            stratioStreamingApi.alterStream(streamName, columns);
            return "Stream ".concat(streamName).concat(" altered correctly");

        } catch (StratioEngineStatusException | StratioAPISecurityException | StratioEngineOperationException e) {
            throw new ShellException(e);
        }
    }
}
