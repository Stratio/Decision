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
package com.stratio.decision.service;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.UUIDs;
import com.stratio.decision.commons.constants.STREAMING;
import com.stratio.decision.commons.messages.ColumnNameTypeValue;
import com.stratio.decision.commons.messages.StratioStreamingMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;


public class SaveToCassandraOperationsService {

    private static final Logger log = LoggerFactory.getLogger(SaveToCassandraOperationsService.class);

    private  final Session session;

    private HashMap<String, Integer> tablenames = new HashMap<>();

    static final String TIMESTAMP_FIELD = "timestamp";

    public SaveToCassandraOperationsService(Session session) {

        this.session = session;

        if (session != null) {
            if (session.getCluster().getMetadata().getKeyspace(STREAMING.STREAMING_KEYSPACE_NAME) == null) {
                createKeyspace(STREAMING.STREAMING_KEYSPACE_NAME);
            }
            refreshTablenames();
        }
    }

    public Boolean check() throws Exception {

        if (session == null)
            return false;

        try {
            return session.getState().getConnectedHosts().size() > 0;
        } catch (Exception e) {
            return false;
        }
    }


    public void checkStructure(StratioStreamingMessage message){

        log.warn("CHECKING STRUCTURE: " + message.getStreamName());

            Set<String> columns = getColumnSet(message.getColumns());
            if (getTableNames().get(message.getStreamName())  == null) {
                createTable(message.getStreamName(), message.getColumns(), TIMESTAMP_FIELD);
                refreshTablenames();
            }
            if (getTableNames().get(message.getStreamName()) != columns.hashCode()) {

               alterTable(message.getStreamName(), columns, message.getColumns());
               refreshTablenames();
            }

    }

    public void execute(BatchStatement batch){
        getSession().execute(batch);
    }


    private Set<String> getColumnSet(List<ColumnNameTypeValue> columns) {
        Set<String> columnsSet = new HashSet<>();
        for (ColumnNameTypeValue column : columns) {
            columnsSet.add(column.getColumn());
        }
        columnsSet.add(TIMESTAMP_FIELD);

        return columnsSet;
    }

    public void refreshTablenames() {
        Collection<TableMetadata> tableMetadatas = session.getCluster().getMetadata()
                .getKeyspace(STREAMING.STREAMING_KEYSPACE_NAME).getTables();
        tablenames = new HashMap<>();
        for (TableMetadata tableMetadata : tableMetadatas) {
            Set<String> columns = new HashSet<>();
            for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
                columns.add(columnMetadata.getName());
            }
            tablenames.put(tableMetadata.getName(), columns.hashCode());
        }
    }



    public void createKeyspace(String keyspaceName) {
        session.execute(String.format(
                "CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
                addQuotes(keyspaceName)));
    }

    public void createTable(String tableName, List<ColumnNameTypeValue> columns, String timestampColumnName) {
        StringBuilder sb = new StringBuilder();

        for (Entry<String, String> entry : getStreamFieldsAndTypes(columns).entrySet()) {
            sb.append(addQuotes(entry.getKey()));
            sb.append(" ");
            sb.append(entry.getValue());
            sb.append(",");
        }
        try {
            session.execute(String
                    .format("CREATE TABLE %s.%s (%s timeuuid, %s PRIMARY KEY (%s)) WITH compression = {'sstable_compression': ''}",
                            STREAMING.STREAMING_KEYSPACE_NAME, addQuotes(tableName), addQuotes(timestampColumnName),
                            sb.toString(), addQuotes(timestampColumnName)));
        } catch (AlreadyExistsException e) {
            log.info("Stream table {} already exists", tableName);
        }
    }

    public Insert createInsertStatement(String streamName, List<ColumnNameTypeValue> columns, String timestampColumnName) {
        Insert insert = QueryBuilder.insertInto(addQuotes(STREAMING.STREAMING_KEYSPACE_NAME), addQuotes(streamName));
        for (ColumnNameTypeValue column : columns) {
            insert.value(addQuotes(column.getColumn()), column.getValue());
        }
        insert.value(addQuotes(timestampColumnName), UUIDs.timeBased());
        return insert;
    }

    public void alterTable(String streamName, Set<String> oldColumnNames, List<ColumnNameTypeValue> columns) {

        StringBuilder sb = new StringBuilder();

        for (Entry<String, String> entry : getStreamFieldsAndTypes(columns).entrySet()) {
            if (!oldColumnNames.contains(entry.getKey())) {
                sb.append(String.format("ALTER TABLE %s.%s ADD %s %s;", STREAMING.STREAMING_KEYSPACE_NAME,
                        addQuotes(streamName), addQuotes(entry.getKey()), entry.getValue()));
            }
        }
        if (!"".equals(sb.toString())) {
            session.execute(sb.toString());
        }
    }

    private String addQuotes(String source) {
        return "\"".concat(source).concat("\"");
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

    public HashMap<String, Integer> getTableNames(){
        return tablenames;
    }

    public Session getSession(){
        return session;
    }



}
