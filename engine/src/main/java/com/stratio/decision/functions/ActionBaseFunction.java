/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.decision.functions;

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.decision.clustering.ClusterSyncManager;
import com.stratio.decision.commons.constants.ReplyCode;
import com.stratio.decision.commons.dto.ActionCallbackDto;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.exception.RequestValidationException;
import com.stratio.decision.exception.StreamExistsException;
import com.stratio.decision.functions.validator.RequestValidation;
import com.stratio.decision.functions.validator.StreamAllowedValidation;
import com.stratio.decision.functions.validator.StreamNameNotNullValidation;
import com.stratio.decision.service.StreamOperationService;
import com.stratio.decision.utils.ZKUtils;

public abstract class ActionBaseFunction implements Function<JavaRDD<StratioStreamingMessage>, Void> {

    private static final long serialVersionUID = -4662722852153543463L;

    protected static Logger log = LoggerFactory.getLogger(ActionBaseFunction.class);

    private final Set<RequestValidation> stopValidators;
    private final Set<RequestValidation> startValidators;
    protected final transient StreamOperationService streamOperationService;
    private final String zookeeperHost;

    public ActionBaseFunction(StreamOperationService streamOperationService, String zookeeperHost) {
        this.stopValidators = new LinkedHashSet<>();
        this.startValidators = new LinkedHashSet<>();
        this.streamOperationService = streamOperationService;
        this.zookeeperHost = zookeeperHost;

        startValidators.add(new StreamNameNotNullValidation());
        startValidators.add(new StreamAllowedValidation());

        stopValidators.add(new StreamNameNotNullValidation());
        stopValidators.add(new StreamAllowedValidation());

        addStartRequestsValidations(startValidators);
        addStopRequestsValidations(stopValidators);
    }

    @Override
    public Void call(JavaRDD<StratioStreamingMessage> rdd) throws Exception {

        rdd.foreach(new VoidFunction<StratioStreamingMessage>() {
            @Override public void call(StratioStreamingMessage message) throws Exception {


//                try {
//
//                    boolean defaultResponse = false;
//
//                    defaultResponse = startAction(message);
//
//                    if (defaultResponse) {
//                        ackStreamingOperation(message, new ActionCallbackDto(ReplyCode.OK.getCode()));
//                    }
//
//                } catch (RequestValidationException e) {
//                    log.error("Custom validation error", e);
//                    ackStreamingOperation(message, new ActionCallbackDto(e.getCode(), e.getMessage()));
//                } catch (Exception e) {
//                    log.error("Fatal validation error", e);
//                    ackStreamingOperation(message,
//                            new ActionCallbackDto(ReplyCode.KO_GENERAL_ERROR.getCode(), e.getMessage()));
//                }


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
                        ackStreamingOperation(message, new ActionCallbackDto(ReplyCode.OK.getCode()));
                    }

                } catch (RequestValidationException e) {
                    log.error("Custom validation error", e);
                    ackStreamingOperation(message, new ActionCallbackDto(e.getCode(), e.getMessage()));
                } catch (Exception e) {
                    log.error("Fatal validation error", e);
                    ackStreamingOperation(message,
                            new ActionCallbackDto(ReplyCode.KO_GENERAL_ERROR.getCode(), e.getMessage()));
                }


            }
        });


//        for (StratioStreamingMessage message : rdd.collect()) {
//            try {
//
//                boolean defaultResponse = false;
//                if (getStartOperationCommand() != null
//                        && getStartOperationCommand().equalsIgnoreCase(message.getOperation())) {
//                    if (validOperation(message, startValidators)) {
//                        defaultResponse = startAction(message);
//                    }
//                } else if (getStopOperationCommand() != null
//                        && getStopOperationCommand().equalsIgnoreCase(message.getOperation())) {
//                    if (validOperation(message, stopValidators)) {
//                        defaultResponse = stopAction(message);
//                    }
//                }
//
//                if (defaultResponse) {
//                    ackStreamingOperation(message, new ActionCallbackDto(ReplyCode.OK.getCode()));
//                }
//
//            } catch (RequestValidationException e) {
//                log.error("Custom validation error", e);
//                ackStreamingOperation(message, new ActionCallbackDto(e.getCode(), e.getMessage()));
//            } catch (Exception e) {
//                log.error("Fatal validation error", e);
//                ackStreamingOperation(message,
//                        new ActionCallbackDto(ReplyCode.KO_GENERAL_ERROR.getCode(), e.getMessage()));
//            }
//        }
        return null;
    }

    private boolean validOperation(StratioStreamingMessage request, Set<RequestValidation> validators) throws Exception {
        log.debug("Validating request operation {} in session id {}", request.getRequest_id(), request.getSession_id());
        for (RequestValidation validation : validators) {
            try {
                validation.validate(request);
            } catch (StreamExistsException e)  {
                log.warn("Stream already exists: " + e.getMessage());
                ackStreamingOperation(request, new ActionCallbackDto(e.getCode(), e.getMessage()));
                return false;
            } catch (RequestValidationException e) {
                log.error("Action validation error", e);
                ackStreamingOperation(request, new ActionCallbackDto(e.getCode(), e.getMessage()));
                return false;
            }
        }
        return true;
    }

    protected void ackStreamingOperation(StratioStreamingMessage message, ActionCallbackDto reply) throws Exception {
//        ZKUtils.getZKUtils(zookeeperHost).createZNodeJsonReply(message, reply);

        ClusterSyncManager.getNode().manageAckStreamingOperation(message, reply);

    }

    public StreamOperationService getStreamOperationService() {
        // return streamOperationService;
        return (StreamOperationService) ActionBaseContext.getInstance().getContext().getBean
        ("streamOperationService");
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
