package com.stratio.streaming.functions.dal;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;

import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.StratioStreamingBaseFunction;
import com.stratio.streaming.streams.StreamOperations;
import com.stratio.streaming.streams.StreamSharedStatus;
import com.stratio.streaming.utils.SiddhiUtils;

public class IndexStreamFunction extends StratioStreamingBaseFunction {

    private static final long serialVersionUID = 3192446933764058322L;

    private static Logger logger = LoggerFactory.getLogger(IndexStreamFunction.class);

    private final String elasticSearchHost;
    private final int elasticSearchPort;

    public IndexStreamFunction(SiddhiManager siddhiManager, String zookeeperCluster, String kafkaCluster,
            String elasticSearchHost, int elasticSearchPort) {
        super(siddhiManager, zookeeperCluster, kafkaCluster);
        this.elasticSearchHost = elasticSearchHost;
        this.elasticSearchPort = elasticSearchPort;
    }

    @Override
    public Void call(JavaRDD<StratioStreamingMessage> rdd) throws Exception {

        List<StratioStreamingMessage> requests = rdd.collect();
        for (StratioStreamingMessage request : requests) {
            if (SiddhiUtils.isStreamAllowedForThisOperation(request.getStreamName(), STREAM_OPERATIONS.ACTION.INDEX)
                    && getSiddhiManager().getStreamDefinition(request.getStreamName()) != null) {

                if (StreamSharedStatus.getStreamStatus(request.getStreamName(), getSiddhiManager()) != null
                        && !StreamSharedStatus.getStreamStatus(request.getStreamName(), getSiddhiManager())
                                .isStreamToIndexer_enabled()) {

                    try {

                        StreamOperations.streamToIndexer(request, elasticSearchHost, elasticSearchPort,
                                getSiddhiManager());

                        ackStreamingOperation(request, REPLY_CODES.OK);

                    } catch (Exception e) {
                        logger.info("<<<<<<<<<<<<<<<<<" + e.getMessage() + "//" + e.getClass() + "//" + e.getCause(), e);
                        ackStreamingOperation(request, REPLY_CODES.KO_GENERAL_ERROR);
                    }
                } else {
                    ackStreamingOperation(request, REPLY_CODES.KO_INDEX_STREAM_ALREADY_ENABLED);
                }

            } else {
                ackStreamingOperation(request, REPLY_CODES.KO_STREAM_DOES_NOT_EXIST);
            }
        }
        return null;
    }

}
