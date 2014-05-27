package com.stratio.streaming.shell.dao;

import java.util.List;

import com.stratio.streaming.commons.exceptions.StratioAPISecurityException;
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException;
import com.stratio.streaming.commons.exceptions.StratioEngineStatusException;
import com.stratio.streaming.commons.streams.StratioStream;
import com.stratio.streaming.messaging.ColumnNameType;

public interface CachedStreamsDAO {

    List<StratioStream> listStreams() throws StratioEngineStatusException;

    void newStream(String name, List<ColumnNameType> columns) throws StratioEngineStatusException,
            StratioAPISecurityException, StratioEngineOperationException;

    void dropStream(String name) throws StratioEngineStatusException, StratioAPISecurityException,
            StratioEngineOperationException;

    List<StratioStream> listUncachedStreams() throws StratioEngineStatusException;

}
