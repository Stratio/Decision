package com.stratio.streaming.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.stratio.streaming.commons.constants.STREAMING;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;

public class CassandraTableOperationsService {

    private static final Logger log = LoggerFactory.getLogger(CassandraTableOperationsService.class);

    private final Session session;

    public CassandraTableOperationsService(Session session) {
        this.session = session;
    }

    public void createKeyspace(String keyspaceName) {
        session.execute(String.format(
                "CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
                keyspaceName));
    }

    public void createTable(String streamName, List<ColumnNameTypeValue> columns, String timestampColumnName) {
        StringBuilder sb = new StringBuilder();

        for (Entry<String, String> entry : getStreamFieldsAndTypes(columns).entrySet()) {
            sb.append(entry.getKey());
            sb.append(" ");
            sb.append(entry.getValue());
            sb.append(",");
        }
        try {
            session.execute(String
                    .format("CREATE TABLE %s.%s (%s timeuuid, %s PRIMARY KEY (%s)) WITH compression = {'sstable_compression': ''}",
                            STREAMING.STREAMING_KEYSPACE_NAME, streamName, timestampColumnName, sb.toString(),
                            timestampColumnName));
        } catch (AlreadyExistsException e) {
            log.info("Stream table {} already exists", streamName);
        }
    }

    public void alterTable(String streamName, Set<String> oldColumnNames, List<ColumnNameTypeValue> columns) {

        StringBuilder sb = new StringBuilder();

        for (Entry<String, String> entry : getStreamFieldsAndTypes(columns).entrySet()) {
            if (!oldColumnNames.contains(entry.getKey())) {
                sb.append(String.format("ALTER TABLE %s.%s ADD %s %s;", STREAMING.STREAMING_KEYSPACE_NAME, streamName,
                        entry.getKey(), entry.getValue()));
            }
        }
        if (!"".equals(sb.toString())) {
            session.execute(sb.toString());
        }
    }

    private HashMap<String, String> getStreamFieldsAndTypes(List<ColumnNameTypeValue> columns) {

        HashMap<String, String> fields = new HashMap<String, String>();
        for (ColumnNameTypeValue column : columns) {
            switch (column.getType()) {
            case BOOLEAN:
                fields.put(column.getColumn(), DataType.Name.BOOLEAN.toString());
                break;
            case DOUBLE:
                fields.put(column.getColumn(), DataType.Name.DOUBLE.toString());
                break;
            case FLOAT:
                fields.put(column.getColumn(), DataType.Name.FLOAT.toString());
                break;
            case INTEGER:
                fields.put(column.getColumn(), DataType.Name.INT.toString());
                break;
            case LONG:
                fields.put(column.getColumn(), DataType.Name.DOUBLE.toString());
                break;
            case STRING:
                fields.put(column.getColumn(), DataType.Name.TEXT.toString());
                break;
            default:
                throw new RuntimeException("Unsupported Column type");
            }
        }

        return fields;
    }
}
