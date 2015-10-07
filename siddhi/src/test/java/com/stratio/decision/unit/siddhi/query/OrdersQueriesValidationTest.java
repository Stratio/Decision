package com.stratio.decision.unit.siddhi.query;

import com.stratio.decision.unit.siddhi.query.model.LineStream;
import com.stratio.decision.unit.siddhi.query.model.OrderStream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
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
 * Created by aitor on 9/16/15.
 */
public class OrdersQueriesValidationTest extends SiddhiQueryHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrdersQueriesValidationTest.class);

    private AtomicInteger count= new AtomicInteger(0);

    private static final String DATASET_ORDERS1= "src/test/resources/orders/dataset-orders-1.csv";
    private static final String DATASET_ORDERS2= "src/test/resources/orders/dataset-orders-2.csv";
    private static final String DATASET_LINES1= "src/test/resources/orders/dataset-lines-1.csv";

    private static String ORDER_QUERY1_ID;
    private static String ORDER_QUERY2_ID;
    private static String ORDER_QUERY3_ID;
    private static String ORDER_QUERY4_ID;
    private static String LINES_QUERY1_ID;
    private static String LINES_QUERY2_ID;

    @BeforeClass
    public static void setUp() throws Exception {
        LOGGER.info("Initializing SiddhiManager");
        initializeSiddhiManager();
        LOGGER.info("Creating Streams");
        createOrdersStream();
        createLinesStream();

        //createOrdersQueries();
        //createLinesQueries();

    }

    @AfterClass
    public static void tearDown() throws Exception {
        LOGGER.info("Shutting down SiddhiManager");
        shutdownSiddhiManager();
    }

    @Test
    public void testStreamsExist() throws Exception {
        LOGGER.debug("Checking if Streams has been created properly");
        assertNotNull(sm.getStreamDefinition(OrdersQueries.STREAM_ORDERS));
        assertNotNull(sm.getStreamDefinition(OrdersQueries.STREAM_LINES));

    }

    @Test
    public void testQueriesExist() throws Exception {
        createOrdersQueries();
        createLinesQueries();
        LOGGER.debug("Checking if Queries has been registered properly");
        assertNotNull(sm.getQuery(ORDER_QUERY1_ID));
        assertNotNull(sm.getQuery(ORDER_QUERY2_ID));
        assertNotNull(sm.getQuery(ORDER_QUERY3_ID));
        assertNotNull(sm.getQuery(ORDER_QUERY4_ID));
        assertNotNull(sm.getQuery(LINES_QUERY1_ID));
        assertNotNull(sm.getQuery(LINES_QUERY2_ID));

        // Streams created as a result of a query
        assertNotNull(sm.getStreamDefinition(OrdersQueries.STREAM_FRAUD));
        assertNotNull(sm.getStreamDefinition(OrdersQueries.STREAM_ALERTS));

        LOGGER.debug("Checking if Queries has been deleted properly");
        deleteOrdersQueries();
        deleteLinesQueries();
        try {
            assertNull(sm.getQuery(ORDER_QUERY1_ID));
            assertNull(sm.getQuery(ORDER_QUERY2_ID));
            assertNull(sm.getQuery(ORDER_QUERY3_ID));
            assertNull(sm.getQuery(ORDER_QUERY4_ID));
            assertNull(sm.getQuery(LINES_QUERY1_ID));
            assertNull(sm.getQuery(LINES_QUERY2_ID));
        } catch (NullPointerException ex)   {
            LOGGER.debug("Shiddhi Manager raise a NullPointerException if you ask for a " +
                    "non existing query so if you read this message all works fine!!");
        }
    }

    //Query 1
    @Test
    public void testFraudMoreThanOneOrderIn5Minutes() throws Exception {
        count.set(0);
        LOGGER.debug("[Fraud] Checking if there are more than one order in 5 minutes");
        LOGGER.debug("--> Creating Order Queries and Loading dataset");
        //createOrdersQueries();
        ORDER_QUERY1_ID= sm.addQuery(OrdersQueries.QUERY_MORE_1ORDER_IN_5M);

        sm.addCallback(OrdersQueries.STREAM_FRAUD, new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    if (event instanceof InEvent && event.getData(6).toString().equals("orders-1")) {
                        count.getAndIncrement();
                        LOGGER.debug("Found event: " + event.toString());
                    }
                }
            }
        });

        loadOrders(DATASET_ORDERS1);

        Thread.sleep(500);
        assertEquals(1, count.get());
        sm.removeQuery(ORDER_QUERY1_ID);

    }

    //Query 2
    @Test
    public void testFraudMoreThanOneOrderIn5MinutesWithSameCard() throws Exception {
        count.set(0);
        LOGGER.debug(
                "[Fraud] Checking if there are more than one order in 5 minutes with same credit cards in different cities");
        LOGGER.debug("--> Creating Order Queries and Loading dataset");
        //createOrdersQueries();
        ORDER_QUERY2_ID= sm.addQuery(OrdersQueries.QUERY_MORE_1ORDER_IN_5M_SAME_CARD);

        sm.addCallback(OrdersQueries.STREAM_FRAUD, new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    if (event instanceof InEvent && event.getData(6).toString().equals("orders-2")) {
                        count.getAndIncrement();
                        LOGGER.debug("Found event: " + event.toString());
                    }
                }
            }
        });

        loadOrders(DATASET_ORDERS1);

        Thread.sleep(500);
        assertEquals(1, count.get());
        sm.removeQuery(ORDER_QUERY2_ID);
    }

    //Query 3
    @Test
    public void testFraudMoreThanOneOrderIn10MinutesWithDifferentCards() throws Exception {
        count.set(0);
        LOGGER.debug("[Fraud] Checking if there are more than one order in 10 minutes with different credit cards");
        LOGGER.debug("--> Creating Order Queries and Loading dataset");
        //createOrdersQueries();
        ORDER_QUERY3_ID= sm.addQuery(OrdersQueries.QUERY_MORE_1_ORDER_IN_10M);

        sm.addCallback(OrdersQueries.STREAM_FRAUD, new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    if (event instanceof InEvent && event.getData(6).toString().equals("orders-3")) {
                        count.getAndIncrement();
                        LOGGER.debug("Found event: " + event.toString());
                    }
                }
            }
        });

        loadOrders(DATASET_ORDERS2);

        Thread.sleep(500);
        assertEquals(2, count.get());
        sm.removeQuery(ORDER_QUERY3_ID);
    }


    //Query 4
    //TODO: Review this query because it's not giving the expecting results
    @Test
    @Ignore
    public void testFraudMoreThanTwoSalesMore10ProdIn20Min() throws Exception {
        count.set(0);
        LOGGER.debug("[Fraud] Checking if there are more than two sales in 20 minutes with 10 products");
        LOGGER.debug("--> Creating Order Queries and Loading dataset");
        //createOrdersQueries();
        ORDER_QUERY4_ID= sm.addQuery(OrdersQueries.QUERY_MORE_2_SALES_10P_IN_20M);

        sm.addCallback(OrdersQueries.STREAM_FRAUD, new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    if (event instanceof InEvent) { //&& event.getData(6).toString().equals("orders-3")) {
                        count.getAndIncrement();
                        LOGGER.debug("Found event: " + event.toString());
                    }
                }
            }
        });

        loadOrders(DATASET_ORDERS2);

        Thread.sleep(500);
        assertEquals(1, count.get());
        sm.removeQuery(ORDER_QUERY4_ID);
    }



    //Query 5
    @Test
    public void testLinesFraudMoreThan3SalesBelow20() throws Exception {
        count.set(0);
        LOGGER.debug(
                "[Fraud] Checking if there are More than 3 sales with total amount below 20€ in less than 10 minutes");
        LOGGER.debug("--> Creating Lines Queries and Loading dataset");
        //createOrdersQueries();
        LINES_QUERY1_ID= sm.addQuery(OrdersQueries.QUERY_MORE_3SALES_BELOW20_10MIN);

        sm.addCallback(OrdersQueries.STREAM_FRAUD, new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    if (event instanceof InEvent && event.getData(6).toString().equals("lines-1")) {
                        count.getAndIncrement();
                        LOGGER.debug("Found event: " + event.toString());
                    }
                }
            }
        });

        loadLines(DATASET_LINES1);

        Thread.sleep(500);
        assertEquals(1, count.get());
        sm.removeQuery(LINES_QUERY1_ID);
    }

    //Query 6
    @Test
    public void testLinesAlertMore15PercentQuantity() throws Exception {
        count.set(0);
        LOGGER.debug(
                "[Alert] Checking if there are alerts about purchases with 15% higher of a same product average purchase");
        LOGGER.debug("--> Creating Lines Queries and Loading dataset");
        //createOrdersQueries();
        LINES_QUERY2_ID= sm.addQuery(OrdersQueries.QUERY_AVG_QUANTITY_HIGHER_15P_10MIN);

        sm.addCallback(OrdersQueries.STREAM_ALERTS, new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    if (event instanceof InEvent && event.getData(6).toString().equals("lines-2")) {
                        count.getAndIncrement();
                        LOGGER.debug("Found event: " + event.toString());
                    }
                }
            }
        });

        loadLines(DATASET_LINES1);

        Thread.sleep(500);
        assertEquals(3, count.get());
        sm.removeQuery(LINES_QUERY2_ID);
    }



    // SUPPORT METHODS

    public static void loadOrders(String dataSetPath) throws Exception {
        LOGGER.debug("Loading orders from file: " + dataSetPath);
        List<String[]> orders= ResourcesLoader.loadData(dataSetPath, ',', '\0', 1);
        List<OrderStream> listOrders= OrderStream.getFromList(orders);

        for (OrderStream order: listOrders) {
            LOGGER.debug("Loading data [" + OrdersQueries.STREAM_ORDERS + "]: " + order.order_id());
            sm.getInputHandler(OrdersQueries.STREAM_ORDERS).send(getOrderStreamAdapted(order));
        }

    }

    public static void loadLines(String dataSetPath) throws Exception {
        LOGGER.debug("Loading lines from file: " + dataSetPath);
        List<String[]> lines= ResourcesLoader.loadData(dataSetPath, ',', '\0', 1);
        List<LineStream> listLines= LineStream.getFromList(lines);

        for (LineStream line: listLines) {
            LOGGER.debug("Loading data [" + OrdersQueries.STREAM_LINES + "]: " + line.order().order_id());
            sm.getInputHandler(OrdersQueries.STREAM_LINES).send(getLineStreamAdapted(line));
        }

    }

    public static Object[] getOrderStreamAdapted(OrderStream order)  {
        return new Object[]{
                order.order_id(), order.timestamp(), order.day_time_zone(),
                order.client_id(), order.payment_method(), order.latitude(),
                order.longitude(), order.credit_card(), order.shopping_center(),
                order.channel(), order.city(), order.country(), order.employee(),
                order.total_amount(), order.total_products(), order.order_size(),
                order.lines()
        };

    }

    public static Object[] getLineStreamAdapted(LineStream line)  {
        OrderStream order= line.order();
        return new Object[]{
                order.order_id(), order.timestamp(), order.day_time_zone(),
                order.client_id(), order.payment_method(), order.latitude(),
                order.longitude(), order.credit_card(), order.shopping_center(),
                order.channel(), order.city(), order.country(), order.employee(),
                order.total_amount(), order.total_products(), order.order_size(),
                line.product(), line.family(), line.quantity(), line.price()
        };

    }


    public static void createOrdersStream() throws Exception {
        try {
            sm.removeStream(OrdersQueries.STREAM_ORDERS);
            sm.removeStream(OrdersQueries.STREAM_FRAUD);
        } catch (Exception ex)  { LOGGER.debug("Error trying to remove the stream, it already exists");}

        sm.defineStream(OrdersQueries.QUERY_CREATE_STREAM_ORDERS);
    }

    public static void createLinesStream() throws Exception {
        try {
            sm.removeStream(OrdersQueries.STREAM_LINES);
            sm.removeStream(OrdersQueries.STREAM_ALERTS);
        } catch (Exception ex)  { LOGGER.debug("Error trying to remove the stream, it already exists");}

        sm.defineStream(OrdersQueries.QUERY_CREATE_STREAM_LINES);    }


    public static void createOrdersQueries() throws Exception {

        ORDER_QUERY1_ID= sm.addQuery(OrdersQueries.QUERY_MORE_1ORDER_IN_5M);
        ORDER_QUERY2_ID= sm.addQuery(OrdersQueries.QUERY_MORE_1ORDER_IN_5M_SAME_CARD);
        ORDER_QUERY3_ID= sm.addQuery(OrdersQueries.QUERY_MORE_1_ORDER_IN_10M);
        ORDER_QUERY4_ID= sm.addQuery(OrdersQueries.QUERY_MORE_2_SALES_10P_IN_20M);

    }

    public static void deleteOrdersQueries() throws Exception {
        sm.removeQuery(ORDER_QUERY1_ID);
        sm.removeQuery(ORDER_QUERY2_ID);
        sm.removeQuery(ORDER_QUERY3_ID);
        sm.removeQuery(ORDER_QUERY4_ID);

    }

    public static void createLinesQueries() throws Exception {

        LINES_QUERY1_ID= sm.addQuery(OrdersQueries.QUERY_MORE_3SALES_BELOW20_10MIN);
        LINES_QUERY2_ID= sm.addQuery(OrdersQueries.QUERY_AVG_QUANTITY_HIGHER_15P_10MIN);

    }

    public static void deleteLinesQueries() throws Exception {

        sm.removeQuery(LINES_QUERY1_ID);
        sm.removeQuery(LINES_QUERY2_ID);

    }

}