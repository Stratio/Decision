package com.stratio.streaming.functions.validator;

import com.stratio.streaming.commons.constants.StreamAction;

public class MongoStreamColumnValidator extends StreamColumnsByRegularExpressionValidator {

    public MongoStreamColumnValidator() {
        super("^[^/\\. \"*<>:|?]+$", StreamAction.SAVE_TO_MONGO);
    }

}
