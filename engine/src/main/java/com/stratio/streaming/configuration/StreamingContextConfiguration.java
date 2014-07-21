package com.stratio.streaming.configuration;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import com.stratio.streaming.StreamingEngine;
import com.stratio.streaming.commons.constants.BUS;
import com.stratio.streaming.commons.constants.STREAMING;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.constants.StreamAction;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.dal.IndexStreamFunction;
import com.stratio.streaming.functions.dal.ListenStreamFunction;
import com.stratio.streaming.functions.dal.SaveToCassandraStreamFunction;
import com.stratio.streaming.functions.dal.SaveToMongoStreamFunction;
import com.stratio.streaming.functions.ddl.AddQueryToStreamFunction;
import com.stratio.streaming.functions.ddl.AlterStreamFunction;
import com.stratio.streaming.functions.ddl.CreateStreamFunction;
import com.stratio.streaming.functions.dml.InsertIntoStreamFunction;
import com.stratio.streaming.functions.dml.ListStreamsFunction;
import com.stratio.streaming.functions.messages.FilterMessagesByOperationFunction;
import com.stratio.streaming.functions.messages.KeepPayloadFromMessageFunction;
import com.stratio.streaming.functions.requests.CollectRequestForStatsFunction;
import com.stratio.streaming.functions.requests.SaveRequestsToAuditLogFunction;
import com.stratio.streaming.streams.QueryDTO;
import com.stratio.streaming.streams.StreamPersistence;
import com.stratio.streaming.streams.StreamSharedStatus;

@Configuration
@Import(StreamingSiddhiConfiguration.class)
public class StreamingContextConfiguration {

    private static Logger logger = LoggerFactory.getLogger(StreamingContextConfiguration.class);

    @Autowired
    private ConfigurationContext configurationContext;

    @Autowired
    private SiddhiManager siddhiManager;

    private JavaStreamingContext create(String streamingContextName, int port) {
        SparkConf conf = new SparkConf();
        conf.set("spark.ui.port", String.valueOf(port));
        conf.set("spark.tachyonStore.folderName", streamingContextName);
        conf.setAppName(streamingContextName);
        conf.setJars(JavaStreamingContext.jarOfClass(StreamingEngine.class));
        conf.setMaster(configurationContext.getSparkHost());

        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(
                configurationContext.getStreamingBatchTime()));

        return streamingContext;
    }

    @Bean(name = "actionContext", destroyMethod = "stop")
    // TODO refactor, please...
    public JavaStreamingContext actionContext() {
        JavaStreamingContext context = this.create("stratio-streaming-action", 4040);

        KeepPayloadFromMessageFunction keepPayloadFromMessageFunction = new KeepPayloadFromMessageFunction();
        CreateStreamFunction createStreamFunction = new CreateStreamFunction(siddhiManager,
                configurationContext.getZookeeperHostsQuorum());
        AlterStreamFunction alterStreamFunction = new AlterStreamFunction(siddhiManager,
                configurationContext.getZookeeperHostsQuorum());
        AddQueryToStreamFunction addQueryToStreamFunction = new AddQueryToStreamFunction(siddhiManager,
                configurationContext.getZookeeperHostsQuorum());
        ListenStreamFunction listenStreamFunction = new ListenStreamFunction(siddhiManager,
                configurationContext.getZookeeperHostsQuorum(), configurationContext.getKafkaHostsQuorum());
        ListStreamsFunction listStreamsFunction = new ListStreamsFunction(siddhiManager,
                configurationContext.getZookeeperHostsQuorum());
        SaveToCassandraStreamFunction saveToCassandraStreamFunction = new SaveToCassandraStreamFunction(siddhiManager,
                configurationContext.getZookeeperHostsQuorum(), configurationContext.getCassandraHostsQuorum());

        Map<String, Integer> baseTopicMap = new HashMap<String, Integer>();
        baseTopicMap.put(BUS.TOPIC_REQUEST, 1);

        JavaPairDStream<String, String> messages = KafkaUtils.createStream(context,
                configurationContext.getZookeeperHostsQuorum(), BUS.STREAMING_GROUP_ID, baseTopicMap);

        messages.cache();

        if (configurationContext.getElasticSearchHost() != null) {
            IndexStreamFunction indexStreamFunction = new IndexStreamFunction(siddhiManager,
                    configurationContext.getZookeeperHostsQuorum(), configurationContext.getElasticSearchHost(),
                    configurationContext.getElasticSearchPort());

            JavaDStream<StratioStreamingMessage> streamToIndexerRequests = messages.filter(
                    new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.INDEX)).map(
                    keepPayloadFromMessageFunction);

            JavaDStream<StratioStreamingMessage> stopStreamToIndexerRequests = messages.filter(
                    new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.STOP_INDEX)).map(
                    keepPayloadFromMessageFunction);

            streamToIndexerRequests.foreachRDD(indexStreamFunction);

            stopStreamToIndexerRequests.foreachRDD(indexStreamFunction);
        } else {
            logger.warn("Elasticsearch configuration not found.");
        }

        if (configurationContext.getMongoHost() != null) {
            SaveToMongoStreamFunction saveToMongoStreamFunction = new SaveToMongoStreamFunction(siddhiManager,
                    configurationContext.getZookeeperHostsQuorum(), configurationContext.getMongoHost(),
                    configurationContext.getMongoPort(), configurationContext.getMongoUsername(),
                    configurationContext.getMongoPassword());

            JavaDStream<StratioStreamingMessage> saveToMongoRequests = messages.filter(
                    new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.SAVETO_MONGO)).map(
                    keepPayloadFromMessageFunction);

            JavaDStream<StratioStreamingMessage> stopSaveToMongoRequests = messages.filter(
                    new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.STOP_SAVETO_MONGO)).map(
                    keepPayloadFromMessageFunction);

            saveToMongoRequests.foreachRDD(saveToMongoStreamFunction);

            stopSaveToMongoRequests.foreachRDD(saveToMongoStreamFunction);
        } else {
            logger.warn("Mongodb configuration not found.");
        }

        // Create a DStream for each command, so we can treat all related
        // requests in the same way and also apply functions by command
        JavaDStream<StratioStreamingMessage> createRequests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.DEFINITION.CREATE)).map(
                keepPayloadFromMessageFunction);

        JavaDStream<StratioStreamingMessage> alterRequests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.DEFINITION.ALTER)).map(
                keepPayloadFromMessageFunction);

        JavaDStream<StratioStreamingMessage> addQueryRequests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.DEFINITION.ADD_QUERY)).map(
                keepPayloadFromMessageFunction);

        JavaDStream<StratioStreamingMessage> removeQueryRequests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.DEFINITION.REMOVE_QUERY)).map(
                keepPayloadFromMessageFunction);

        JavaDStream<StratioStreamingMessage> listenRequests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.LISTEN)).map(
                keepPayloadFromMessageFunction);

        JavaDStream<StratioStreamingMessage> stopListenRequests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.STOP_LISTEN)).map(
                keepPayloadFromMessageFunction);

        JavaDStream<StratioStreamingMessage> saveToCassandraRequests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.SAVETO_CASSANDRA)).map(
                keepPayloadFromMessageFunction);

        JavaDStream<StratioStreamingMessage> stopSaveToCassandraRequests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.STOP_SAVETO_CASSANDRA)).map(
                keepPayloadFromMessageFunction);

        JavaDStream<StratioStreamingMessage> listRequests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.MANIPULATION.LIST)).map(
                keepPayloadFromMessageFunction);

        JavaDStream<StratioStreamingMessage> dropRequests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.DEFINITION.DROP)).map(
                keepPayloadFromMessageFunction);

        createRequests.foreachRDD(createStreamFunction);
        alterRequests.foreachRDD(alterStreamFunction);
        addQueryRequests.foreachRDD(addQueryToStreamFunction);
        removeQueryRequests.foreachRDD(addQueryToStreamFunction);
        listenRequests.foreachRDD(listenStreamFunction);
        stopListenRequests.foreachRDD(listenStreamFunction);
        saveToCassandraRequests.foreachRDD(saveToCassandraStreamFunction);
        stopSaveToCassandraRequests.foreach(saveToCassandraStreamFunction);
        listRequests.foreachRDD(listStreamsFunction);
        dropRequests.foreachRDD(createStreamFunction);

        if (configurationContext.isAuditEnabled() || configurationContext.isStatsEnabled()) {

            JavaDStream<StratioStreamingMessage> allRequests = createRequests.union(alterRequests)
                    .union(addQueryRequests).union(removeQueryRequests).union(listenRequests).union(stopListenRequests)
                    .union(saveToCassandraRequests).union(listRequests).union(dropRequests);

            if (configurationContext.isAuditEnabled()) {
                SaveRequestsToAuditLogFunction saveRequestsToAuditLogFunction = new SaveRequestsToAuditLogFunction(
                        siddhiManager, configurationContext.getZookeeperHostsQuorum(),
                        configurationContext.getKafkaHostsQuorum(), configurationContext.getCassandraHostsQuorum());

                // persist the RDDs to cassandra using STRATIO DEEP
                allRequests.window(new Duration(2000), new Duration(6000)).foreachRDD(saveRequestsToAuditLogFunction);
            }

            if (configurationContext.isStatsEnabled()) {
                CollectRequestForStatsFunction collectRequestForStatsFunction = new CollectRequestForStatsFunction(
                        siddhiManager, configurationContext.getZookeeperHostsQuorum(),
                        configurationContext.getKafkaHostsQuorum());

                allRequests.window(new Duration(2000), new Duration(6000)).foreachRDD(collectRequestForStatsFunction);

            }
        }

        StreamPersistence.saveStreamingEngineStatus(siddhiManager);

        if (configurationContext.isPrintStreams()) {

            // DEBUG STRATIO STREAMING ENGINE //
            messages.count().foreach(new Function<JavaRDD<Long>, Void>() {

                private static final long serialVersionUID = -2371501158355376325L;

                @Override
                public Void call(JavaRDD<Long> arg0) throws Exception {
                    StringBuffer sb = new StringBuffer();
                    sb.append("\n********************************************\n");
                    sb.append("**            SIDDHI STREAMS              **\n");
                    sb.append("** countSiddhi:");
                    sb.append(siddhiManager.getStreamDefinitions().size());
                    sb.append(" // countHazelcast: ");
                    sb.append(siddhiManager.getSiddhiContext().getHazelcastInstance()
                            .getMap(STREAMING.STREAM_STATUS_MAP).size());
                    sb.append("     **\n");

                    for (StreamDefinition streamMetaData : siddhiManager.getStreamDefinitions()) {

                        StringBuffer streamDefinition = new StringBuffer();

                        streamDefinition.append(streamMetaData.getStreamId());

                        for (Attribute column : streamMetaData.getAttributeList()) {
                            streamDefinition.append(" |" + column.getName() + "," + column.getType());
                        }

                        if (StreamSharedStatus.getStreamStatus(streamMetaData.getStreamId(), siddhiManager) != null) {
                            HashMap<String, QueryDTO> attachedQueries = StreamSharedStatus.getStreamStatus(
                                    streamMetaData.getStreamId(), siddhiManager).getAddedQueries();

                            streamDefinition.append(" /// " + attachedQueries.size() + " attachedQueries: (");

                            for (String queryId : attachedQueries.keySet()) {
                                streamDefinition.append(queryId + "/");
                            }

                            streamDefinition.append(" - userDefined:"
                                    + StreamSharedStatus.getStreamStatus(streamMetaData.getStreamId(), siddhiManager)
                                            .isUserDefined() + "- ");
                            streamDefinition.append(" - listenEnable:"
                                    + StreamSharedStatus.getStreamStatus(streamMetaData.getStreamId(), siddhiManager)
                                            .isActionEnabled(StreamAction.LISTEN) + "- ");
                        }

                        sb.append("** stream: ".concat(streamDefinition.toString()).concat("\n"));
                    }

                    sb.append("********************************************\n");

                    logger.info(sb.toString());

                    StreamPersistence.saveStreamingEngineStatus(siddhiManager);

                    return null;
                }

            });
        }

        return context;
    }

    @Bean(name = "dataContext", destroyMethod = "stop")
    // TODO refactor, please...
    public JavaStreamingContext dataContext() {
        JavaStreamingContext context = this.create("stratio-streaming-data", 4041);

        KeepPayloadFromMessageFunction keepPayloadFromMessageFunction = new KeepPayloadFromMessageFunction();

        Map<String, Integer> baseTopicMap = new HashMap<String, Integer>();
        baseTopicMap.put(BUS.TOPIC_DATA, 1);

        JavaPairDStream<String, String> messages = KafkaUtils.createStream(context,
                configurationContext.getZookeeperHostsQuorum(), BUS.STREAMING_GROUP_ID, baseTopicMap);

        messages.cache();

        JavaDStream<StratioStreamingMessage> insertRequests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.MANIPULATION.INSERT)).map(
                keepPayloadFromMessageFunction);

        InsertIntoStreamFunction insertIntoStreamFunction = new InsertIntoStreamFunction(siddhiManager,
                configurationContext.getZookeeperHostsQuorum());

        insertRequests.foreachRDD(insertIntoStreamFunction);

        return context;

    }

    // @Bean(name = "processContext", destroyMethod = "stop")
    // TODO new context, not used yet
    public JavaStreamingContext processContext() {
        return this.create("stratio-streaming-process", 4042);
    }

}
