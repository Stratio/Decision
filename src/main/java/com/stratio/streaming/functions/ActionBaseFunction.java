package com.stratio.streaming.functions;

import java.util.HashSet;
import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;

import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.dto.ActionCallbackDto;
import com.stratio.streaming.exception.RequestValidationException;
import com.stratio.streaming.functions.validator.RequestValidation;
import com.stratio.streaming.functions.validator.StreamExistsValidation;
import com.stratio.streaming.utils.ZKUtils;

public abstract class ActionBaseFunction extends Function<JavaRDD<StratioStreamingMessage>, Void> {

    private static final long serialVersionUID = -4662722852153543463L;

    private static Logger log = LoggerFactory.getLogger(ActionBaseFunction.class);

    private final Set<RequestValidation> validators;
    private final transient SiddhiManager siddhiManager;
    private final String zookeeperHost;

    public ActionBaseFunction(SiddhiManager siddhiManager, String zookeeperHost) {
        this.validators = new HashSet<>();
        this.siddhiManager = siddhiManager;
        this.zookeeperHost = zookeeperHost;

        validators.add(new StreamExistsValidation(siddhiManager));
        addRequestsValidations(validators);
    }

    @Override
    public Void call(JavaRDD<StratioStreamingMessage> rdd) throws Exception {
        for (StratioStreamingMessage message : rdd.collect()) {
            try {
                if (validOperation(message)) {
                    if (getStartOperationCommand().equals(message.getOperation())) {
                        startAction(message);
                    } else if (getStopOperationCommand().equals(message.getOperation())) {
                        stopAction(message);
                    }
                    ackStreamingOperation(message, new ActionCallbackDto(REPLY_CODES.OK));
                }
            } catch (Exception e) {
                ackStreamingOperation(message, new ActionCallbackDto(REPLY_CODES.KO_GENERAL_ERROR, e.getMessage()));
            }
        }
        return null;
    }

    private boolean validOperation(StratioStreamingMessage request) throws Exception {
        log.debug("Validating request operation {} in session id {}", request.getRequest_id(), request.getSession_id());
        for (RequestValidation validation : validators) {
            try {
                validation.validate(request);
            } catch (RequestValidationException e) {
                ackStreamingOperation(request, new ActionCallbackDto(e.getCode(), e.getMessage()));
                return false;
            }
        }
        return true;
    }

    private void ackStreamingOperation(StratioStreamingMessage message, ActionCallbackDto reply) throws Exception {
        // TODO check this refactor
        // if
        // (!request.getOperation().equalsIgnoreCase(STREAM_OPERATIONS.MANIPULATION.INSERT))
        // {
        ZKUtils.getZKUtils(zookeeperHost).createZNodeJsonReply(message, reply);
        // }
    }

    public SiddhiManager getSiddhiManager() {
        return siddhiManager;
    }

    protected abstract String getStartOperationCommand();

    protected abstract String getStopOperationCommand();

    protected abstract void startAction(StratioStreamingMessage message);

    protected abstract void stopAction(StratioStreamingMessage message);

    protected void addRequestsValidations(Set<RequestValidation> validators) {
    }

}
