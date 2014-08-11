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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.UUIDs;
import com.stratio.streaming.commons.constants.STREAMING;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.service.CassandraTableOperationsService;

public class SaveToCassandraActionExecutionFunction extends BaseActionExecutionFunction {

    private static final long serialVersionUID = -3116164624590830333L;

    private Session cassandraSession;

    private final String cassandraQuorum;

    private HashMap<String, Integer> tablenames = new HashMap<>();

    private CassandraTableOperationsService cassandraTableOperationsService;

    public SaveToCassandraActionExecutionFunction(String cassandraQuorum) {
        this.cassandraQuorum = cassandraQuorum;
    }

    @Override
    public void process(Iterable<StratioStreamingMessage> messages) throws Exception {
        BatchStatement batch = new BatchStatement();
        for (StratioStreamingMessage stratioStreamingMessage : messages) {
            Set<String> columns = getColumnSet(stratioStreamingMessage.getColumns());
            if (tablenames.get(stratioStreamingMessage.getStreamName().toLowerCase()) == null) {
                getCassandraTableOperationsService().createTable(stratioStreamingMessage.getStreamName(),
                        stratioStreamingMessage.getColumns(), TIMESTAMP_FIELD);
                refreshTablenames();
            }
            if (tablenames.get(stratioStreamingMessage.getStreamName().toLowerCase()) != columns.hashCode()) {
                getCassandraTableOperationsService().alterTable(stratioStreamingMessage.getStreamName(), columns,
                        stratioStreamingMessage.getColumns());
                refreshTablenames();
            }

            Insert insert = QueryBuilder.insertInto(STREAMING.STREAMING_KEYSPACE_NAME,
                    stratioStreamingMessage.getStreamName());
            for (ColumnNameTypeValue column : stratioStreamingMessage.getColumns()) {
                insert.value(column.getColumn(), column.getValue());
            }
            insert.value(TIMESTAMP_FIELD, UUIDs.timeBased());
            batch.add(insert);
        }

        getSession().execute(batch);
    }

    private CassandraTableOperationsService getCassandraTableOperationsService() {
        if (cassandraTableOperationsService == null) {
            cassandraTableOperationsService = new CassandraTableOperationsService(getSession());
        }

        return cassandraTableOperationsService;
    }

    private Session getSession() {
        if (cassandraSession == null) {
            cassandraSession = Cluster.builder().addContactPoints(cassandraQuorum.split(",")).build().connect();
            if (cassandraSession.getCluster().getMetadata().getKeyspace(STREAMING.STREAMING_KEYSPACE_NAME) == null) {
                getCassandraTableOperationsService().createKeyspace(STREAMING.STREAMING_KEYSPACE_NAME);
            }
            refreshTablenames();
        }
        return cassandraSession;
    }

    private void refreshTablenames() {
        Collection<TableMetadata> tableMetadatas = getSession().getCluster().getMetadata()
                .getKeyspace(STREAMING.STREAMING_KEYSPACE_NAME).getTables();
        tablenames = new HashMap<>();
        for (TableMetadata tableMetadata : tableMetadatas) {
            Set<String> columns = new HashSet<>();
            for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
                columns.add(columnMetadata.getName().toLowerCase());
            }
            tablenames.put(tableMetadata.getName(), columns.hashCode());
        }
    }

    private Set<String> getColumnSet(List<ColumnNameTypeValue> columns) {
        Set<String> columnsSet = new HashSet<>();
        for (ColumnNameTypeValue column : columns) {
            columnsSet.add(column.getColumn().toLowerCase());
        }
        columnsSet.add(TIMESTAMP_FIELD.toLowerCase());

        return columnsSet;
    }
}
