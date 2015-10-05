package com.stratio.decision.service;

import com.stratio.decision.commons.constants.StreamAction;
import com.stratio.decision.dao.StreamStatusDao;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by aitor on 9/22/15.
 */
public class StreamStatusMetricServiceTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamStatusMetricServiceTest.class);

    private StreamStatusMetricService service;


    @Before
    public void setUp() throws Exception {
        LOGGER.debug("Initializing required classes");
        StreamStatusDao dao= new StreamStatusDao();

        dao.create(StreamsHelper.STREAM_NAME, StreamsHelper.COLUMNS);
        dao.create(StreamsHelper.STREAM_NAME2, StreamsHelper.COLUMNS2);

        dao.enableAction(StreamsHelper.STREAM_NAME, StreamAction.LISTEN);

        dao.addQuery(StreamsHelper.STREAM_NAME, StreamsHelper.QUERY_ID, StreamsHelper.QUERY);
        dao.addQuery(StreamsHelper.STREAM_NAME2, StreamsHelper.QUERY_ID2, StreamsHelper.QUERY2);

        service= new StreamStatusMetricService(dao);
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testGetTotalStreams() throws Exception {
        int number= service.getTotalStreams();
        assertEquals(2, number);

    }

    @Test
    public void testGetStreamNames() throws Exception {
        List<String> listNames= service.getStreamNames();

        assertTrue("Expected true not found", listNames.contains(StreamsHelper.STREAM_NAME));
        assertTrue("Expected true not found", listNames.contains(StreamsHelper.STREAM_NAME2));
    }

    @Test
    public void testGetStreamActions() throws Exception {
        Map<String, String> listActions= service.getStreamActions();
        assertEquals("Expected value not found", 2, listActions.size());
        assertTrue("Expected true not found", listActions.containsKey(StreamsHelper.STREAM_NAME));
        assertTrue("Expected true not found", listActions.get(StreamsHelper.STREAM_NAME).equals(StreamsHelper.ACTION_LISTEN_TOKEN));
    }
}