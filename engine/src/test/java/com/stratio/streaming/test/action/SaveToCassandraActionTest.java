package com.stratio.streaming.test.action;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.stratio.streaming.commons.constants.ColumnType;
import com.stratio.streaming.commons.constants.STREAMING;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.SaveToCassandraActionExecutionFunction;

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
        saveToCassandraActionExecutionFunction = new SaveToCassandraActionExecutionFunction("localhost", 9042);
    }

    @After
    public void after() throws IOException {
        if (cassandraServer != null) {
            cassandraServer.shutdown();
        }
    }

    @Test
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
