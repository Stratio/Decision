package com.stratio.streaming.functions;

import java.util.HashSet;
import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;

import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.dto.ActionCallbackDto;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.exception.RequestValidationException;
import com.stratio.streaming.functions.validator.RequestValidation;
import com.stratio.streaming.functions.validator.StreamAllowedValidation;
import com.stratio.streaming.utils.ZKUtils;

public abstract class ActionBaseFunction extends Function<JavaRDD<StratioStreamingMessage>, Void> {

    private static final long serialVersionUID = -4662722852153543463L;

    protected static Logger log = LoggerFactory.getLogger(ActionBaseFunction.class);

    private final Set<RequestValidation> stopValidators;
    private final Set<RequestValidation> startValidators;
    private final transient SiddhiManager siddhiManager;
    private final String zookeeperHost;

    public ActionBaseFunction(SiddhiManager siddhiManager, String zookeeperHost) {
        this.stopValidators = new HashSet<>();
        this.startValidators = new HashSet<>();
        this.siddhiManager = siddhiManager;
        this.zookeeperHost = zookeeperHost;

        startValidators.add(new StreamAllowedValidation(getSiddhiManager()));
        stopValidators.add(new StreamAllowedValidation(getSiddhiManager()));
        addStartRequestsValidations(startValidators);
        addStopRequestsValidations(stopValidators);
    }

    @Override
    public Void call(JavaRDD<StratioStreamingMessage> rdd) throws Exception {
        for (StratioStreamingMessage message : rdd.collect()) {
            try {

                boolean defaultResponse = false;
                if (getStartOperationCommand() != null
                        && getStartOperationCommand().equalsIgnoreCase(message.getOperation())) {
                    if (validOperation(message, startValidators)) {
                        defaultResponse = startAction(message);
                    }
                } else if (getStopOperationCommand() != null
                        && getStopOperationCommand().equalsIgnoreCase(message.getOperation())) {
                    if (validOperation(message, stopValidators)) {
                        defaultResponse = stopAction(message);
                    }
                }

                if (defaultResponse) {
                    ackStreamingOperation(message, new ActionCallbackDto(REPLY_CODES.OK));
                }

            } catch (RequestValidationException e) {
                log.error("Custom validation error", e);
                ackStreamingOperation(message, new ActionCallbackDto(e.getCode(), e.getMessage()));
            } catch (Exception e) {
                log.error("Fatal validation error", e);
                ackStreamingOperation(message, new ActionCallbackDto(REPLY_CODES.KO_GENERAL_ERROR, e.getMessage()));
            }
        }
        return null;
    }

    private boolean validOperation(StratioStreamingMessage request, Set<RequestValidation> validators) throws Exception {
        log.debug("Validating request operation {} in session id {}", request.getRequest_id(), request.getSession_id());
        for (RequestValidation validation : validators) {
            try {
                validation.validate(request);
            } catch (RequestValidationException e) {
                log.error("Action validation error", e);
                ackStreamingOperation(request, new ActionCallbackDto(e.getCode(), e.getMessage()));
                return false;
            }
        }
        return true;
    }

    protected void ackStreamingOperation(StratioStreamingMessage message, ActionCallbackDto reply) throws Exception {
        ZKUtils.getZKUtils(zookeeperHost).createZNodeJsonReply(message, reply);
    }

    public SiddhiManager getSiddhiManager() {
        return siddhiManager;
    }

    public String getZookeeperHost() {
        return zookeeperHost;
    }

    /**
     * Start operation command to execute this start action
     * 
     * @return string represents operation command
     */
    protected abstract String getStartOperationCommand();

    /**
     * Stop operation command to execute this stop action
     * 
     * @return string represents operation command
     */
    protected abstract String getStopOperationCommand();

    protected abstract boolean startAction(StratioStreamingMessage message) throws RequestValidationException;

    protected abstract boolean stopAction(StratioStreamingMessage message) throws RequestValidationException;

    protected abstract void addStopRequestsValidations(Set<RequestValidation> validators);

    protected abstract void addStartRequestsValidations(Set<RequestValidation> validators);

}
