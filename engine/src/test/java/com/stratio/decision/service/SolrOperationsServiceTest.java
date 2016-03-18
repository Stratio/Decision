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

import com.stratio.decision.commons.messages.StratioStreamingMessage;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static org.junit.Assert.assertTrue;

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

        service= new SolrOperationsService(HOSTS, HOSTS, DATA_FOLDER.getRoot().getAbsolutePath(), IS_CLOUD);

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