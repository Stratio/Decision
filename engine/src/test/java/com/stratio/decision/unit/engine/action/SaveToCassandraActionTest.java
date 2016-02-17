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
package com.stratio.decision.unit.engine.action;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.stratio.decision.commons.constants.ColumnType;
import com.stratio.decision.commons.constants.STREAMING;
import com.stratio.decision.commons.messages.ColumnNameTypeValue;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.functions.SaveToCassandraActionExecutionFunction;
import org.junit.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SaveToCassandraActionTest {

    private SaveToCassandraActionExecutionFunction saveToCassandraActionExecutionFunction;

    private CassandraServer cassandraServer;
    private Session cassandraSession;

    private static final String UPPERCASE_STREAM_NAME = "TEST_STREAM_NAME";
    private static final String UPPERCASE_STREAM_COLUMN = "TEST_COLUMN_NAME";

    @Before
    public void before() throws Exception {
        cassandraServer = new CassandraServer();
        cassandraServer.start();
        saveToCassandraActionExecutionFunction = new SaveToCassandraActionExecutionFunction("localhost", 9042, 50,
                BatchStatement.Type.UNLOGGED);
    }

    @After
    public void after() throws IOException {
        if (cassandraServer != null) {
            cassandraServer.shutdown();
        }
    }

    @Test
    @Ignore
    // TODO fix test with new cassandra version
    public void tableNameCanBeUppercaseTest() throws Exception {
        saveToCassandraActionExecutionFunction.process(getSimpleList());

        Assert.assertTrue(getSession().getCluster().getMetadata().getKeyspace(STREAMING.STREAMING_KEYSPACE_NAME)
                .getTable("\"" + UPPERCASE_STREAM_NAME + "\"") != null);

        Assert.assertTrue(getSession()
                .execute(
                        QueryBuilder.select().from(STREAMING.STREAMING_KEYSPACE_NAME,
                                "\"" + UPPERCASE_STREAM_NAME + "\"")).one().getColumnDefinitions()
                .contains(UPPERCASE_STREAM_COLUMN));

    }

    private List<StratioStreamingMessage> getSimpleList() {
        List<StratioStreamingMessage> result = new ArrayList<>();
        StratioStreamingMessage message = new StratioStreamingMessage();
        message.setStreamName(UPPERCASE_STREAM_NAME);
        message.addColumn(new ColumnNameTypeValue(UPPERCASE_STREAM_COLUMN, ColumnType.INTEGER, 0));
        result.add(message);
        return result;
    }

    private Session getSession() {
        if (cassandraSession == null) {
            cassandraSession = Cluster.builder().addContactPoints("localhost").withPort(9042).build().connect();
        }
        return cassandraSession;
    }
}
