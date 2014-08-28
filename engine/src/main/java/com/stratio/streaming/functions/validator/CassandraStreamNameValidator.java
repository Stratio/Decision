package com.stratio.streaming.functions.validator;

import com.stratio.streaming.commons.constants.StreamAction;

public class CassandraStreamNameValidator extends StreamNameByRegularExpressionValidator {

    public CassandraStreamNameValidator() {
        super("^[A-Za-z][\\w,_,0-9]*", StreamAction.SAVE_TO_CASSANDRA);
    }

}
