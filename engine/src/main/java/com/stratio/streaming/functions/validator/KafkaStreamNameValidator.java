package com.stratio.streaming.functions.validator;

import com.stratio.streaming.commons.constants.StreamAction;

public class KafkaStreamNameValidator extends StreamNameByRegularExpressionValidator {

    public KafkaStreamNameValidator() {
        super("^[a-zA-Z0-9_-]+$", StreamAction.LISTEN);
    }

}
