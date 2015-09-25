package com.stratio.streaming.service;

import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * Created by aitor on 9/23/15.
 */
public class SolrOperationsServiceTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SolrOperationsServiceTest.class);

    private SolrOperationsService service;

    private static final String HOSTS= "localhost";

    private static final Boolean IS_CLOUD= false;

    @Rule
    public TemporaryFolder DATA_FOLDER = new TemporaryFolder();

    @Rule
    public TemporaryFolder CONF_FOLDER = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        LOGGER.debug("Initializing required classes");

        service= new SolrOperationsService(HOSTS, DATA_FOLDER.getRoot().getAbsolutePath(), IS_CLOUD);

    }

    /**
    * TODO: SolrOperationService create SolrConnections under the hood so it's difficult to mock
    * the Solr connections. It would be great to refactor SolrOperationService to receive SolrClient
    * as a parameter, allowing to mock that object and create a good test case
     */
    @Test
    @Ignore
    public void testCreateCore() throws Exception {

        StratioStreamingMessage message= new StratioStreamingMessage(
                    StreamsHelper.STREAM_NAME, Long.parseLong("1234567890"), StreamsHelper.COLUMNS);
            service.createCore(message);
    }

    @Test
    public void testCreateDirs() throws Exception {
        String testDataDir= DATA_FOLDER.getRoot().getAbsolutePath() + File.separator + "testData";
        String testConfDir= DATA_FOLDER.getRoot().getAbsolutePath() + File.separator + "testConf";
        service.createDirs(testDataDir, testConfDir);
        assertTrue("Expected true value not found", new File(testDataDir).isDirectory());
        assertTrue("Expected true value not found", new File(testConfDir).isDirectory());
    }

    @Test
    @Ignore
    public void testCreateSolrSchema() throws Exception {

        service.createSolrSchema(StreamsHelper.COLUMNS, CONF_FOLDER.getRoot().getAbsolutePath());
        File schemaFile= new File(CONF_FOLDER.getRoot().getAbsolutePath() + File.separator + "schema.xml");
        assertTrue("Expected true value not found", schemaFile.canRead());
    }

}