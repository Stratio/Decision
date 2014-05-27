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

}
