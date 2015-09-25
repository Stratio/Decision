package com.stratio.streaming.service;

import com.datastax.driver.core.querybuilder.Insert;
import com.stratio.streaming.commons.constants.ColumnType;
import com.stratio.streaming.commons.constants.STREAMING;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import org.apache.commons.collections.set.ListOrderedSet;
import org.glassfish.grizzly.utils.ArraySet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.Session;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * Created by aitor on 9/23/15.
 * TODO:
 * Current SaveToCassandraOperationsService uses datastax Session object but doesn't return anything
 * in the method implementations. To build a better integration and create better test it would be
 * nice refactor that code to process the returned data and validate the information.
 */
public class SaveToCassandraOperationsServiceTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SaveToCassandraOperationsServiceTest.class);

    private SaveToCassandraOperationsService service;

    private final String KEYSPACE= "mykeyspace";

    private final String TABLE= "mytable";

    private final List<ColumnNameTypeValue> columns= new ArrayList<ColumnNameTypeValue>();
    {
        columns.add(new ColumnNameTypeValue("id", ColumnType.INTEGER, 1));
        columns.add(new ColumnNameTypeValue("name", ColumnType.STRING, "my name"));
        columns.add(new ColumnNameTypeValue("enabled", ColumnType.BOOLEAN, "true"));
        columns.add(new ColumnNameTypeValue("mydouble", ColumnType.DOUBLE, 1.0));
        columns.add(new ColumnNameTypeValue("myfloat", ColumnType.FLOAT, 1f));
        columns.add(new ColumnNameTypeValue("mylong", ColumnType.LONG, Long.parseLong("1")));
        columns.add(new ColumnNameTypeValue("timestamp", ColumnType.STRING, "1234567890"));
    }

    @Mock
    private Session mockedSession;

    @Before
    public void setUp() throws Exception {
        LOGGER.debug("Initializing required classes");
        mockedSession= mock(Session.class);
        service= new SaveToCassandraOperationsService(mockedSession);

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testCreateKeyspace() throws Exception {
        service.createKeyspace(KEYSPACE);
        Exception ex= null;
        try {
            service.createKeyspace(KEYSPACE);
        } catch (Exception e)  {
            ex= e;
        }
        assertEquals("Expected null but exception found", null, ex);
    }

    @Test
    public void testCreateTable() throws Exception {
        Exception ex= null;
        try {
            service.createTable(TABLE, columns, "timestamp");
        } catch (Exception e)  {
            ex= e;
        }
        assertEquals("Expected null but exception found", null, ex);
    }

    @Test
    public void testCreateInsertStatement() throws Exception {
        Insert insert= service.createInsertStatement(TABLE, columns, "timestamp");
        assertEquals("Expected keyspace not found",
                "\"" + STREAMING.STREAMING_KEYSPACE_NAME + "\"", insert.getKeyspace());

    }

    @Test
    public void testAlterTable() throws Exception {
        Exception ex= null;
        try {

            columns.add(new ColumnNameTypeValue("newfield", ColumnType.STRING, "my new value"));
            Set<String> oldColumnNamesnew= new ListOrderedSet();
            oldColumnNamesnew.add("id");
            oldColumnNamesnew.add("name");
            oldColumnNamesnew.add("enabled");
            oldColumnNamesnew.add("timestamp");

            service.alterTable(TABLE, oldColumnNamesnew, columns);

        } catch (Exception e)  {
            ex= e;
        }
        assertEquals("Expected null but exception found", null, ex);

    }
}