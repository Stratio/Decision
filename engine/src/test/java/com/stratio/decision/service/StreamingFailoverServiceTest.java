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

/**
 * Created by aitor on 9/22/15.
 * TODO: Review to test class injecting needed resources with spring

@RunWith( SpringJUnit4ClassRunner.class )
@ContextConfiguration(classes = {ConfigurationContext.class, StreamingSiddhiConfiguration.class })
public class StreamingFailoverServiceTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingFailoverServiceTest.class);

    private StreamingFailoverService service;

    private StreamStatusDao streamStatusDao;

    private StreamMetadataService streamMetadataService;

    @Mock
    private StreamingFailoverDao mockedDao;

    private SiddhiManager siddhiManager;

    //private StreamOperationService operationService;

    private CallbackService callbackService;



    @Before
    public void setUp() throws Exception {
        LOGGER.debug("Initializing required classes");
        siddhiManager= new StreamingSiddhiConfiguration().siddhiManager();
        streamStatusDao= new StreamStatusDao();
        streamMetadataService= new StreamMetadataService(siddhiManager);

        mockedDao= mock(StreamingFailoverDao.class);

        ServiceConfiguration serviceConfiguration= new ServiceConfiguration();
        callbackService= serviceConfiguration.callbackService();

        //operationService= new StreamOperationService(siddhiManager, streamStatusDao, callbackService);
        service= new StreamingFailoverService(streamStatusDao, streamMetadataService, mockedDao);
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testLoad() throws Exception {
        byte[] bytes= {};
        Map<String, StreamStatusDTO> streamStatuses= new HashMap<>();
        StreamStatusDTO dto= new StreamStatusDTO(StreamsHelper.STREAM_NAME, true, StreamsHelper.COLUMNS);

        streamStatuses.put(StreamsHelper.STREAM_NAME, dto);
        when(mockedDao.load()).thenReturn(new FailoverPersistenceStoreModel(streamStatuses, bytes));

        service.load();
    }

    @Test
    public void testSave() throws Exception {

    }/
}*/