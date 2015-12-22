package com.stratio.decision.functions.validator;

import com.stratio.decision.commons.constants.StreamAction;

/**
 * Created by josepablofernandez on 21/12/15.
 */
public class DroolsStreamNameValidator extends StreamNameByRegularExpressionValidator {

    public DroolsStreamNameValidator() {
        super("^[A-Za-z][\\w,_,0-9]*", StreamAction.SEND_TO_DROOLS);
    }
}
