package com.stratio.streaming.functions.validator;

import org.wso2.siddhi.core.SiddhiManager;

import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.exception.RequestValidationException;
import com.stratio.streaming.streams.StreamSharedStatus;

public class QueryExistsValidation extends BaseSiddhiRequestValidation {

    private final static String QUERY_ALREADY_EXISTS_MESSAGE = "Query in stream %s already exists";

    public QueryExistsValidation(SiddhiManager sm) {
        super(sm);
    }

    @Override
    public void validate(StratioStreamingMessage request) throws RequestValidationException {
        if (StreamSharedStatus.getStreamStatus(request.getStreamName(), getSm()) != null) {
            // TODO normalize query and create their hash to verify correctly if
            // this query exists
            if (StreamSharedStatus.getStreamStatus(request.getStreamName(), getSm()).getAddedQueries()
                    .containsValue(request.getRequest())) {
                throw new RequestValidationException(REPLY_CODES.KO_QUERY_ALREADY_EXISTS, String.format(
                        QUERY_ALREADY_EXISTS_MESSAGE, request.getStreamName()));
            }
        }
    }
}
