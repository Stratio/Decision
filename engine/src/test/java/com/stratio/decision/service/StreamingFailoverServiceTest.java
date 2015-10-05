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