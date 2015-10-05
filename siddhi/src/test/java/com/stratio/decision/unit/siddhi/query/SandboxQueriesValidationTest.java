package com.stratio.decision.unit.siddhi.query;

import com.stratio.decision.unit.siddhi.query.model.SandboxStream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.stream.output.StreamCallback;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * Created by aitor on 9/21/15.
 */
public class SandboxQueriesValidationTest extends SiddhiQueryHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(SandboxQueriesValidationTest.class);

    private AtomicInteger count= new AtomicInteger(0);
    private AtomicInteger countMemory= new AtomicInteger(0);

    private static final String DATASET_SENSORS1= "src/test/resources/sandbox/dataset-sensors-1.csv";
    private static final String DATASET_SENSORS2= "src/test/resources/sandbox/dataset-sensors-2.csv";

    private static String SENSORS_QUERY1_ID;
    private static String SENSORS_QUERY2_ID;
    private static String SENSORS_QUERY3_ID;
    private static String SENSORS_QUERY4_ID;

    @BeforeClass
    public static void setUp() throws Exception {
        LOGGER.info("Initializing SiddhiManager");
        initializeSiddhiManager();
        LOGGER.info("Creating Streams");
        createSandboxStream();

    }

    @AfterClass
    public static void tearDown() throws Exception {
        LOGGER.info("Shutting down SiddhiManager");
        shutdownSiddhiManager();
    }

    @Test
    public void testStreamsExist() throws Exception {
        LOGGER.info("Checking if Streams has been created properly");
        assertNotNull(sm.getStreamDefinition(SandboxQueries.STREAM_SENSORS));

    }

    @Test
    public void testQueriesExist() throws Exception {
        createSandboxQueries();

        LOGGER.info("Checking if Queries has been registered properly");
        assertNotNull(sm.getQuery(SENSORS_QUERY1_ID));
        assertNotNull(sm.getQuery(SENSORS_QUERY2_ID));
        assertNotNull(sm.getQuery(SENSORS_QUERY3_ID));
        assertNotNull(sm.getQuery(SENSORS_QUERY4_ID));


        // Streams created as a result of a query
        assertNotNull(sm.getStreamDefinition(SandboxQueries.STREAM_SENSORS_GRID_AVG));
        assertNotNull(sm.getStreamDefinition(SandboxQueries.STREAM_SENSORS_GRID_ALARMS));

        LOGGER.info("Checking if Queries has been deleted properly");
        deleteSandboxQueries();

        try {
            assertNull(sm.getQuery(SENSORS_QUERY1_ID));
            assertNull(sm.getQuery(SENSORS_QUERY2_ID));
            assertNull(sm.getQuery(SENSORS_QUERY3_ID));
            assertNull(sm.getQuery(SENSORS_QUERY4_ID));

        } catch (NullPointerException ex)   {
            LOGGER.info("Shiddhi Manager raise a NullPointerException if you ask for a " +
                    "non existing query so if you read this message all works fine!!");
        }
    }

    //Query 1
    @Test
    public void testAvgLast250() throws Exception {
        count.set(0);
        LOGGER.info("[AVG] Getting last events");
        LOGGER.info("--> Creating Sandbox Queries and Loading dataset");
        SENSORS_QUERY1_ID= sm.addQuery(SandboxQueries.QUERY_250_AVG);

        sm.addCallback(SandboxQueries.STREAM_SENSORS_GRID_AVG, new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    if (event instanceof InEvent) {
                        count.getAndIncrement();
                        LOGGER.info("Found event: " + event.toString());
                    }
                }
            }
        });

        loadSensorsData(DATASET_SENSORS1);

        Thread.sleep(500);
        assertEquals(300, count.get());
        sm.removeQuery(SENSORS_QUERY1_ID);

    }


    //Query 2
    @Test
    public void testAvgCpuHigherThan80() throws Exception {
        count.set(0);
        LOGGER.info("[AVG] Raising alarm if AVG(CPU) > 80% ");
        LOGGER.info("--> Creating Sandbox Queries and Loading dataset");
        SENSORS_QUERY1_ID= sm.addQuery(SandboxQueries.QUERY_250_AVG);
        SENSORS_QUERY2_ID= sm.addQuery(SandboxQueries.QUERY_AVG_CPU_HIGHER_80);

        sm.addCallback(SandboxQueries.STREAM_SENSORS_GRID_ALARMS, new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    if (event instanceof InEvent && event.getData(2).toString().equals("Alarm_intensive_CPU_load")) {
                        count.getAndIncrement();
                        LOGGER.info("Found event: " + event.toString());
                    }
                }
            }
        });

        loadSensorsData(DATASET_SENSORS1);

        Thread.sleep(2000);
        assertEquals(1, count.get());
        sm.removeQuery(SENSORS_QUERY1_ID);
        sm.removeQuery(SENSORS_QUERY2_ID);

    }


    //Query 3
    @Test
    public void testAvgMemoryHigherThan75() throws Exception {
        count.set(0);
        LOGGER.info("[AVG] Raising alarm if AVG(MEMORY) > 75% ");
        LOGGER.info("--> Creating Sandbox Queries and Loading dataset");
        SENSORS_QUERY1_ID= sm.addQuery(SandboxQueries.QUERY_250_AVG);
        SENSORS_QUERY2_ID= sm.addQuery(SandboxQueries.QUERY_AVG_CPU_HIGHER_80);
        SENSORS_QUERY3_ID= sm.addQuery(SandboxQueries.QUERY_AVG_MEMORY_HIGHER_75);

        sm.addCallback(SandboxQueries.STREAM_SENSORS_GRID_ALARMS, new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    if (event instanceof InEvent && event.getData(2).toString().equals("Alarm_intensive_MEMORY_load")) {
                        count.getAndIncrement();
                        LOGGER.info("Found event: " + event.toString());
                    }
                }
            }
        });

        loadSensorsData(DATASET_SENSORS1);

        Thread.sleep(2000);
        assertEquals(1, count.get());
        sm.removeQuery(SENSORS_QUERY1_ID);
        sm.removeQuery(SENSORS_QUERY2_ID);
        sm.removeQuery(SENSORS_QUERY3_ID);

    }

    //Query 4
    @Test
    public void testAvgHighCpuAndMemory() throws Exception {
        count.set(0);
        LOGGER.info("[AVG] Raising alarm if AVG(CPU) > 90 % and AVG(MEMORY) > 80% ");
        LOGGER.info("--> Creating Sandbox Queries and Loading dataset");
        createSandboxQueries();

        sm.addCallback(SandboxQueries.STREAM_SENSORS_GRID_ALARMS, new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    if (event instanceof InEvent && event.getData(2).toString().equals("Alarm_inminent_shutdown")) {
                        count.getAndIncrement();
                        LOGGER.info("Found event: " + event.toString());
                    }
                }
            }
        });

        loadSensorsData(DATASET_SENSORS1);
        loadSensorsData(DATASET_SENSORS2);

        Thread.sleep(2000);
        assertEquals(1, count.get());
        deleteSandboxQueries();

    }

    // SUPPORT METHODS

    public static void loadSensorsData(String dataSetPath) throws Exception {
        LOGGER.info("Loading sensors data from file: " + dataSetPath);
        List<String[]> sensors= ResourcesLoader.loadData(dataSetPath, ',', '\0', 1);
        List<SandboxStream> listSensors= SandboxStream.getFromList(sensors);

        for (SandboxStream sensor: listSensors) {
            LOGGER.info("Loading data [" + SandboxQueries.STREAM_SENSORS + "]: " + sensor.name() + "=" + sensor.data());
            sm.getInputHandler(SandboxQueries.STREAM_SENSORS).send(getSandboxStreamAdapted(sensor));
        }

    }

    public static Object[] getSandboxStreamAdapted(SandboxStream sandbox)  {
        return new Object[]{
                sandbox.name(), sandbox.data()
        };

    }


    public static void createSandboxStream() throws Exception {
        try {
            sm.removeStream(SandboxQueries.STREAM_SENSORS);
            sm.removeStream(SandboxQueries.STREAM_SENSORS_GRID_AVG);
        } catch (Exception ex)  { LOGGER.debug("Error trying to remove the stream, it already exists");}

        sm.defineStream(SandboxQueries.QUERY_CREATE_STREAM_SENSORS);
    }


    public static void createSandboxQueries() throws Exception {

        SENSORS_QUERY1_ID= sm.addQuery(SandboxQueries.QUERY_250_AVG);
        SENSORS_QUERY2_ID= sm.addQuery(SandboxQueries.QUERY_AVG_CPU_HIGHER_80);
        SENSORS_QUERY3_ID= sm.addQuery(SandboxQueries.QUERY_AVG_MEMORY_HIGHER_75);
        SENSORS_QUERY4_ID= sm.addQuery(SandboxQueries.QUERY_AVG_CPU_OR_MEMORY);

    }

    public static void deleteSandboxQueries() throws Exception {
        sm.removeQuery(SENSORS_QUERY1_ID);
        sm.removeQuery(SENSORS_QUERY2_ID);
        sm.removeQuery(SENSORS_QUERY3_ID);
        sm.removeQuery(SENSORS_QUERY4_ID);

    }

}
