package com.stratio.streaming.functions.validator;

import org.wso2.siddhi.core.SiddhiManager;

import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.exception.RequestValidationException;
import com.stratio.streaming.streams.StreamSharedStatus;

public class QueryNotExistsValidation extends BaseSiddhiRequestValidation {

    private final static String QUERY_DOES_NOT_EXIST_MESSAGE = "Query %s in stream %s does not exists";

    public QueryNotExistsValidation(SiddhiManager sm) {
        super(sm);
    }

    @Override
    public void validate(StratioStreamingMessage request) throws RequestValidationException {
        if (StreamSharedStatus.getStreamStatus(request.getStreamName(), getSm()) != null) {
            // TODO normalize query and create their hash to verify correctly if
            // this query exists
            if (!StreamSharedStatus.getStreamStatus(request.getStreamName(), getSm()).getAddedQueries()
                    .containsKey(request.getRequest())) {
                throw new RequestValidationException(REPLY_CODES.KO_QUERY_DOES_NOT_EXIST, String.format(
                        QUERY_DOES_NOT_EXIST_MESSAGE, request.getRequest(), request.getStreamName()));
            }
        }
    }
}
