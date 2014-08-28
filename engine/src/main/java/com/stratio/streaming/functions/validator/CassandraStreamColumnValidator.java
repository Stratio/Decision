package com.stratio.streaming.functions.validator;

import com.stratio.streaming.commons.constants.StreamAction;

public class CassandraStreamColumnValidator extends StreamColumnsByRegularExpressionValidator {

    public CassandraStreamColumnValidator() {
        super("^[A-Za-z][\\w,_,0-9]*", StreamAction.SAVE_TO_CASSANDRA);
    }

}
