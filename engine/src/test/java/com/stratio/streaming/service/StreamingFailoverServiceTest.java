package com.stratio.streaming.service;

import com.stratio.streaming.configuration.ServiceConfiguration;
import com.stratio.streaming.configuration.StreamingSiddhiConfiguration;
import com.stratio.streaming.dao.StreamStatusDao;
import com.stratio.streaming.dao.StreamingFailoverDao;
import com.stratio.streaming.model.FailoverPersistenceStoreModel;
import com.stratio.streaming.streams.StreamStatusDTO;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.wso2.siddhi.core.SiddhiManager;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by aitor on 9/22/15.
 */
//@RunWith( SpringJUnit4ClassRunner.class )
//@ContextConfiguration(classes = {})
public class StreamingFailoverServiceTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingFailoverServiceTest.class);

    private StreamingFailoverService service;

    private StreamStatusDao streamStatusDao;

    private StreamMetadataService streamMetadataService;

    @Mock
    private StreamingFailoverDao mockedDao;

    private SiddhiManager siddhiManager;

    private StreamOperationService operationService;

    private CallbackService callbackService;

    /**

    @Before
    public void setUp() throws Exception {
        LOGGER.debug("Initializing required classes");
        siddhiManager= new StreamingSiddhiConfiguration().siddhiManager();
        streamStatusDao= new StreamStatusDao();
        streamMetadataService= new StreamMetadataService(siddhiManager);

        mockedDao= mock(StreamingFailoverDao.class);

        ServiceConfiguration serviceConfiguration= new ServiceConfiguration();
        callbackService= serviceConfiguration.callbackService();

        operationService= new StreamOperationService(siddhiManager, streamStatusDao, callbackService);
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

    }
    */
}