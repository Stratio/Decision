package com.stratio.streaming.functions.validator;

import com.stratio.streaming.commons.constants.StreamAction;

public class MongoStreamNameValidator extends StreamNameByRegularExpressionValidator {

    public MongoStreamNameValidator() {
        super("^[^/\\. \"*<>:|?]+$", StreamAction.SAVE_TO_MONGO);
    }

}
