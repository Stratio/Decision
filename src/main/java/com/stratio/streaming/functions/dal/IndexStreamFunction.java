package com.stratio.streaming.functions.dal;

import java.util.Set;

import org.wso2.siddhi.core.SiddhiManager;

import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.ActionBaseFunction;
import com.stratio.streaming.functions.validator.ActionEnabledValidation;
import com.stratio.streaming.functions.validator.RequestValidation;
import com.stratio.streaming.streams.StreamOperations;
import com.stratio.streaming.streams.StreamStatusDTO.StreamAction;

public class IndexStreamFunction extends ActionBaseFunction {

    private static final long serialVersionUID = -689381870050478255L;

    private final String elasticSearchHost;
    private final int elasticSearchPort;

    public IndexStreamFunction(SiddhiManager siddhiManager, String zookeeperHost, String elasticSearchHost,
            int elasticSearchPort) {
        super(siddhiManager, zookeeperHost);
        this.elasticSearchHost = elasticSearchHost;
        this.elasticSearchPort = elasticSearchPort;
    }

    @Override
    protected String getStartOperationCommand() {
        return STREAM_OPERATIONS.ACTION.INDEX;
    }

    @Override
    protected String getStopOperationCommand() {
        return STREAM_OPERATIONS.ACTION.STOP_INDEX;
    }

    @Override
    protected void startAction(StratioStreamingMessage message) {
        StreamOperations.streamToIndexer(message, elasticSearchHost, elasticSearchPort, getSiddhiManager());
    }

    @Override
    protected void stopAction(StratioStreamingMessage message) {
        // TODO Auto-generated method stub

    }

    @Override
    protected void addRequestsValidations(Set<RequestValidation> validators) {
        validators.add(new ActionEnabledValidation(getSiddhiManager(), StreamAction.INDEXED,
                REPLY_CODES.KO_INDEX_STREAM_ALREADY_ENABLED));
    }

}
