package com.stratio.decision.functions;

import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ProtocolOptions;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.functions.dal.ListenStreamFunction;
import com.stratio.decision.service.StreamsHelper;

/**
 * Created by aitor on 9/23/15.
 */
public class FunctionsTestIT extends ActionBaseFunctionHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionsTestIT.class);
    private static JavaSparkContext context = null;

    @Before
    public void setUp() throws Exception {
        LOGGER.debug("Initializing required classes");
        initialize();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        try {
            if (context instanceof JavaSparkContext)
                context.stop();
        } catch (Exception ex) {
        }
    }

    @Test
    public void testActionBaseFunctionCall() throws Exception {

        List<StratioStreamingMessage> list = new ArrayList<StratioStreamingMessage>();

        message.setOperation("stop_listen");
        list.add(message);

        LOGGER.debug("Starting Local Spark Context");
        context = new JavaSparkContext("local[2]", "test");
        JavaRDD<StratioStreamingMessage> rdd = context.parallelize(list);

        ListenStreamFunction func = new ListenStreamFunction(streamOperationsService, ZOO_HOST);
        Exception ex = null;
        try {
            func.startAction(message);
            func.call(rdd);

        } catch (Exception e) {
            ex = e;
        }

        assertNull("Expected null value", ex);
        context.stop();
    }

    @Test
    public void testSaveToMongo() throws Exception {
        LOGGER.debug("Connecting to MongoDB host: " + conf.getStringList("mongo.hosts").toString());

        SaveToMongoActionExecutionFunction func = new SaveToMongoActionExecutionFunction(
                conf.getStringList("mongo.hosts"), null, null, 1000);

        List<StratioStreamingMessage> list = new ArrayList<StratioStreamingMessage>();
        list.add(message);

        Exception ex = null;
        try {
            func.process(list);

        } catch (Exception e) {
            ex = e;
            ex.printStackTrace();
        }

        assertNull("Expected null value", ex);
    }

    @Test
    public void testSendToKafka() throws Exception {
        LOGGER.debug("Connecting to Kafka host: " + conf.getStringList("kafka.hosts").toString());
        SendToKafkaActionExecutionFunction func =
                new SendToKafkaActionExecutionFunction(getHostsStringFromList(conf.getStringList("kafka.hosts")));

        List<StratioStreamingMessage> list = new ArrayList<StratioStreamingMessage>();
        list.add(message);

        Exception ex = null;
        try {
            func.process(list);

        } catch (Exception e) {
            ex = e;
            ex.printStackTrace();
        }

        assertNull("Expected null value", ex);
    }

    @Test
    public void testSaveToCassandra() throws Exception {
        LOGGER.debug("Connecting to Cassandra Quorum: " + conf.getStringList("cassandra.hosts").toString());

        SaveToCassandraActionExecutionFunction func = new SaveToCassandraActionExecutionFunction(
                getHostsStringFromList(conf.getStringList("cassandra.hosts")), ProtocolOptions.DEFAULT_PORT, 50,  BatchStatement.Type.UNLOGGED);

        List<StratioStreamingMessage> list = new ArrayList<StratioStreamingMessage>();
        message.setColumns(StreamsHelper.COLUMNS3);
        list.add(message);

        Exception ex = null;
        try {
            func.process(list);

        } catch (Exception e) {
            ex = e;
            ex.printStackTrace();
        }

        assertNull("Expected null value", ex);
    }


    @Test
    public void testSaveToElasticSearch() throws Exception {
        LOGGER.debug("Connecting to Elastic Search: " + conf.getStringList("elasticsearch.hosts").toString());

        SaveToElasticSearchActionExecutionFunction func = new SaveToElasticSearchActionExecutionFunction(
                conf.getStringList("elasticsearch.hosts"), conf.getString("elasticsearch.clusterName"));

        List<StratioStreamingMessage> list = new ArrayList<StratioStreamingMessage>();
        message.setColumns(StreamsHelper.COLUMNS3);
        list.add(message);

        Exception ex = null;
        try {
            func.process(list);

        } catch (Exception e) {
            ex = e;
            ex.printStackTrace();
        }

        assertNull("Expected null value", ex);
    }


}
